# coopervere/services/danfe_service.py

import os
import requests
from typing import Dict, Any, List
import base64

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

from .database import create_db_engine
from .evolution_api import EvolutionAPI, EvolutionAPIError
from .notifier_service import normalizar_celular_br, notificar_ti_pedido_sem_celular
from .gerar_danfe import gerar_danfe

load_dotenv()


MEUDANFE_URL = os.getenv("API_URL_MEU_DANFE_XML_TO_PDF")
STATUS_PENDENTE = "P"
STATUS_ENVIADO = "E"
STATUS_FALHA   = "F"


class MeuDanfeError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None, payload: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}
        

def converter_xml_para_danfe(xml: str) -> Dict[str, Any]:
    """
    Envia o XML da NFe/CTe para o MeuDanfe e retorna o JSON de resposta.

    Esperado (HTTP 200):
    {
      "name": "NFE-....pdf",
      "type": "NFE",
      "format": "BASE64",
      "data": "<pdf_em_base64>"
    }
    """
    api_key = os.getenv("API_KEY_MEU_DANFE")
    if not api_key:
        raise RuntimeError("API_KEY_MEU_DANFE não definido no .env")

    headers = {
        "Api-Key": api_key,
        # a doc diz que o body é text/plain
        "Content-Type": "text/plain; charset=utf-8",
    }

    resp = requests.post(
        MEUDANFE_URL,
        headers=headers,
        data=xml.encode("utf-8"),
        timeout=60,
    )

    # Tratamento de erro de HTTP
    if resp.status_code != 200:
        try:
            payload = resp.json()
        except Exception:
            payload = {"text": resp.text}
        raise MeuDanfeError(
            f"Erro MeuDanfe HTTP {resp.status_code}",
            status_code=resp.status_code,
            payload=payload,
        )

    try:
        data = resp.json()
    except ValueError:
        raise MeuDanfeError("Resposta do MeuDanfe não é JSON válido.")

    # validação básica
    if "data" not in data:
        raise MeuDanfeError("Resposta do MeuDanfe não contém campo 'data'.")

    return data

def buscar_xml_nfe(chave_acesso: str) -> str:
    """
    Busca o XML da NFe via RETXMLNFE(:CHAVEACESSO) e, se houver,
    combina XML + XMLAUTORIZACAO em um nfeProc completo.
    """

    sql = text("""
        SELECT XML, XMLAUTORIZACAO
        FROM RETXMLNFE(:CHAVEACESSO)
    """)

    eng = create_db_engine()
    with eng.connect() as conn:
        row = conn.execute(sql, {"CHAVEACESSO": chave_acesso}).fetchone()

    if not row:
        raise RuntimeError(f"NFe não encontrada para chave {chave_acesso}")

    xml_nfe = (row[0] or "").strip()
    xml_aut = (row[1] or "").strip() if len(row) > 1 else ""

    if not xml_nfe:
        raise RuntimeError(f"XML da NFe vazio para chave {chave_acesso}")

    # Se já veio um nfeProc completo, não mexe
    if "<nfeProc" in xml_nfe:
        return xml_nfe

    # Remove o prolog <?xml ...?> se vier em qualquer um
    def strip_prolog(x: str) -> str:
        x = x.lstrip()
        if x.startswith("<?xml"):
            fim = x.find("?>")
            if fim != -1:
                x = x[fim+2:].lstrip()
        return x

    xml_nfe = strip_prolog(xml_nfe)
    xml_aut = strip_prolog(xml_aut)

    # Se não tiver XMLAUTORIZACAO, devolve só o XML da NFe mesmo
    if not xml_aut:
        return xml_nfe

    # Monta exatamente no formato que você mostrou como "XML correto":
    xml_completo = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<nfeProc versao="4.00" xmlns="http://www.portalfiscal.inf.br/nfe">'
        f'{xml_nfe}'
        f'{xml_aut}'
        '</nfeProc>'
    )

    return xml_completo



def buscar_notas_pendentes() -> List[dict]:
    """
    Busca registros pendentes na tabela CV_DANFE_VENDA_NOTIFICA.
    Campos principais:
      - CHAVEACESSO
      - NRODOC
      - SERIEDOC
      - MODELO
      - ID_CLIENTE
      - NOME_CLIENTE
      - CEL_CLIENTE
    """
    sql = text("""
        SELECT
            CHAVEACESSO,
            NRODOC,
            SERIEDOC,
            MODELO,
            ID_CLIENTE,
            NOME_CLIENTE,
            CEL_CLIENTE
        FROM CV_DANFE_VENDA_NOTIFICA
        WHERE STATUS = :STATUS_P
    """)

    eng = create_db_engine()
    with eng.connect() as conn:
        rows = conn.execute(sql, {"STATUS_P": STATUS_PENDENTE}).mappings().all()

    return [dict(r) for r in rows]


def atualizar_status_nota(chave_acesso: str, status: str):
    """
    Atualiza STATUS e DTHRENVIO da CV_DANFE_VENDA_NOTIFICA.
    """
    sql = text("""
        UPDATE CV_DANFE_VENDA_NOTIFICA
           SET STATUS    = :STATUS,
               DTHRENVIO = CURRENT_TIMESTAMP
         WHERE CHAVEACESSO = :CHAVEACESSO
    """)

    eng = create_db_engine()
    with eng.begin() as conn:
        conn.execute(sql, {"STATUS": status, "CHAVEACESSO": chave_acesso})

def montar_msg_nfe(nota: dict) -> str:
    """
    Monta a mensagem de texto que irá junto com o PDF da DANFE.
    nota: dict com NOME_CLIENTE, SERIEDOC, NRODOC etc.
    """
    nome = nota.get("nome_cliente") or "Cliente"
    serie = (nota.get("seriedoc") or "").strip()
    numero = (nota.get("nrodoc") or "").strip()

    num_fmt = f"{numero}-{serie}" if serie or numero else "(sem número)"

    return (
        f"Olá {nome}! "
        f"CooperVerê informa o Faturamento da Nota Fiscal Nº {num_fmt}."
    )

def processar_notas_pendentes() -> dict:
    """
    Processa todos registros em CV_DANFE_VENDA_NOTIFICA com STATUS='P':
      - Busca XML via RETXMLNFE(:CHAVEACESSO)
      - Converte em DANFE PDF base64 via MeuDanfe
      - Normaliza telefone do cliente
      - Envia PDF via Evolution API (send_media)
      - Atualiza STATUS para 'E' ou 'F'
    Retorna um resumo com contagens.
    """
    evo = EvolutionAPI()
    pendentes = buscar_notas_pendentes()

    # print(pendentes) # validar o Json dos documentos pendentes.

    enviados = 0
    falhas = 0

    for nota in pendentes:
        chave = nota["chaveacesso"]
        cel_raw = nota.get("cel_cliente") or "" # pega o telefone do cadastro do cliente.
        # cel_raw = "5546999111465" # (telefone para teste)
        nome_cli = nota.get("nome_cliente") or ""
        serie = (nota.get("seriedoc") or "").strip()
        numero = (nota.get("nrodoc") or "").strip()

        try:
            # ================================
            # 1) Obter celular do cliente
            # ================================
            cel_norm = normalizar_celular_br(cel_raw)

            if not cel_norm:
                # sem celular válido: marca falha e avisa TI
                atualizar_status_nota(chave, STATUS_PENDENTE)
                falhas += 1

                try:
                    notificar_ti_pedido_sem_celular(
                        contexto="NF-e",
                        identificador=f"{serie}-{numero}",
                        nome_cliente=nome_cli,
                        celular_original=cel_raw,
                    )
                except Exception as e_ti:
                    print(f"[WARN] Falha ao avisar TI sobre NF {serie}-{numero}: {e_ti}")

                print(f"[WARN] NF {serie}-{numero}: celular inválido '{cel_raw}'")
                continue

           # ================================
            # 2) Obter XML da nota
            # ================================
            xml = buscar_xml_nfe(chave)

            # ================================
            # 3) Gerar DANFE (PDF base64)
            # ================================
            try:
                # ============================================================
                # NOVO MÉTODO — GERAR PDF VIA LIB python `brazilfiscalreport`
                # ============================================================

                pdf_bytes = gerar_danfe(xml)                      # recebe bytes do PDF
                pdf_b64   = base64.b64encode(pdf_bytes).decode()  # converte para base64
                pdf_name  = f"NFE-{chave}.pdf"

            except Exception as e_geral_local:
                print(f"[WARN] Falha ao gerar DANFE localmente, tentando via MeuDanfe: {e_geral_local}")

                # ============================================================
                # MÉTODO ANTIGO — API MeuDanfe (mantido como FAILOVER)
                # ============================================================
                # resp_md = converter_xml_para_danfe(xml)
                # pdf_b64 = resp_md["data"]
                # pdf_name = resp_md.get("name") or f"NFE-{chave}.pdf"

                # Se quiser habilitar temporariamente o método antigo, DESCOMENTE acima ↓
                raise RuntimeError("Falha ao gerar DANFE localmente e fallback desabilitado.")

            # ================================
            # 4) Montar mensagem de texto
            # ================================
            mensagem = montar_msg_nfe(nota)

            # ================================
            # 5) Enviar via Evolution API
            # ================================
            evo.send_media(
                phone=cel_norm,
                mediatype="document",
                mimetype="application/pdf",
                caption=mensagem,
                media=pdf_b64,
                file_name=pdf_name,
            )

            # ================================
            # 6) Marcar como enviado
            # ================================
            atualizar_status_nota(chave, STATUS_ENVIADO)
            enviados += 1

        except EvolutionAPIError as e:
            # Erros vindos da Evolution (inclui HTTP 400 para número inválido)
            falhas += 1
            try:
                atualizar_status_nota(chave, STATUS_PENDENTE)
            except Exception:
                pass

            print(
                f"[ERRO] Evolution ao enviar NF chave {chave}: {e} "
                f"(status={getattr(e, 'status_code', None)}, payload={getattr(e, 'payload', {})})"
            )

            # Se for HTTP 400, muito provavelmente número de WhatsApp inválido → avisa TI
            if getattr(e, "status_code", None) == 400:
                try:
                    notificar_ti_pedido_sem_celular(
                        contexto="NF-e",
                        identificador=f"{serie}-{numero}",
                        nome_cliente=nome_cli,
                        celular_original=cel_norm or cel_raw,
                    )
                except Exception as e_ti:
                    print(f"[WARN] Falha ao avisar TI sobre NF {serie}-{numero} (HTTP 400 Evolution): {e_ti}")

        except (SQLAlchemyError, MeuDanfeError, Exception) as e:
            # Demais erros de banco, MeuDanfe, etc.
            falhas += 1
            try:
                atualizar_status_nota(chave, STATUS_PENDENTE)
            except Exception:
                pass
            print(f"[ERRO] Falha ao enviar NF chave {chave}: {e}")

    return {
        "total": len(pendentes),
        "enviados": enviados,
        "falhas": falhas,
    }