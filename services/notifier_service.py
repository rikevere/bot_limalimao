# codigos/coopervere/services/notifier_service.py
import os
from datetime import datetime
from typing import Tuple
import re

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .database import create_db_engine
from .evolution_api import EvolutionAPI

from .pdf_utils import build_pedido_pdf 


load_dotenv()

STATUS_PENDENTE = "P"
STATUS_ENVIADO = "E"
STATUS_FALHA = "F"

def enviar_pdf_pedido(dados: dict, phone: str) -> dict:
    """
    Gera o PDF do pedido e envia via Evolution API como 'document'.
    """
    api = EvolutionAPI()
    file_name, b64_pdf = build_pedido_pdf(dados)
    return api.send_media(
        phone=phone,
        mediatype="document",
        mimetype="application/pdf",
        #caption=f"Resumo do pedido {dados['header'].get('NUMERO','')}",
        caption=f"CooperVerê - Novo Pedido Faturado",
        media=b64_pdf,
        file_name=file_name,
        link_preview=False,
    )

def fetch_pendentes() -> list[Tuple[int, str, int]]:
    """
    Retorna (ESTAB, SERIE, NUMERO) para linhas com STATUS = 'P'
    """
    sql = text("""
        SELECT ESTAB, SERIE, NUMERO
        FROM CV_PEDCAB_NOTIFICA
        WHERE STATUS = :st AND SERIE = 'PV'
        ORDER BY DATA_CRIACAO
    """)
    eng = create_db_engine()  # você escolheu a Opção 2
    with eng.connect() as conn:
        rows = conn.execute(sql, {"st": STATUS_PENDENTE}).mappings().all()

    out: list[Tuple[int, str, int]] = []
    for r in rows:
        estab  = r.get("ESTAB")  or r.get("estab")
        serie  = r.get("SERIE")  or r.get("serie")
        numero = r.get("NUMERO") or r.get("numero")
        if isinstance(serie, str):
            serie = serie.strip()
        out.append((estab, serie, numero))
    return out

def run_business_query(estab: int, serie: str, numero: int) -> dict:
    """
    Executa a consulta do pedido + itens e retorna um dicionário com 'header' e 'items'.
    header: dados do pedido/cliente
    items:  lista de itens (um por linha de PEDITEM)
    """
    sql = text("""
        SELECT 
            PEDCAB.ESTAB,
            PEDCAB.STATUS, 
            PEDCFG.ENTRADASAIDA,
            PEDCAB.SERIE || '-' || PEDCAB.NUMERO AS NUMERO,
            PEDCAB.DTEMISSAO,
            PEDCAB.DTVALIDADE,
            PEDCAB.DTPREVISAO,
            CASE
                WHEN (PEDCAB.SITUACAO = 0) THEN 'A Pagar'
                WHEN (PEDCAB.SITUACAO = 1) THEN 'Pago'
                WHEN (PEDCAB.SITUACAO = 2) THEN 'Parcialmente Pago'
                ELSE 'Todos'
            END AS SITUACAO,                                                 
            PEDCAB.PESSOA || '-' || CONTAMOV.NOME AS NOME,
            CONTAMOV.CELULAR,
            COALESCE(ENDERECO.ENDERECO, CONTAMOV.ENDERECO) || ', ' ||
            COALESCE(ENDERECO.NUMEROEND, CONTAMOV.NUMEROEND) || ', ' ||
            CIDADE.NOME || '-' || CIDADE.UF AS ENDERECO_COMP,
            PEDITEM.SEQPEDITE,
            ITEMAGRO.DESCRICAO AS ITEMDESCRICAO,
            ITEMMARCA.DESCRICAO AS MARCA,  
            PEDITEM.QUANTIDADE - PEDITEM.CANCELADO AS QUANTIDADE,
            COALESCE(PEMBALAGEM.UNDESTINO, ITEMAGRO.UNIDADE) AS UNIDADE,

            /* Safe-divide substituindo DIVIDE(...) e NVLF0(...) */
            COALESCE(
            (
                (PEDITEM.VALORUNITARIO + COALESCE(PEDITEM.VLRUNITFRETE, 0))
                - COALESCE( COALESCE(PEDITEM.DESCONTO, 0) / NULLIF(PEDITEM.QUANTIDADE, 0), 0 )
            ),
            COALESCE(
                COALESCE(PEDITEM.VALOR, 0) /
                NULLIF( (COALESCE(PEDITEM.QUANTIDADE,0) - COALESCE(PEDITEM.CANCELADO,0)), 0 ),
                0
            )
            ) AS VALORUNITARIO,

            /* Safe-divide já aplicado para desconto proporcional */
            (
            PEDITEM.VALOR - (
                COALESCE(
                COALESCE(PEDCAB.DESCONTOMERCADORIA, 0) /
                NULLIF(PEDCAB.VALORMERCADORIA + COALESCE(PEDCAB.DESCONTOMERCADORIA, 0), 0),
                0
                ) * PEDITEM.VALOR
            )
            ) AS VALOR,
            PEDCAB.VALORMERCADORIA AS VALOR_TOTAL_PEDIDO

        FROM PEDCAB
        INNER JOIN PEDITEM
            ON PEDCAB.ESTAB = PEDITEM.ESTAB
            AND PEDCAB.SERIE = PEDITEM.SERIE
            AND PEDCAB.NUMERO = PEDITEM.NUMERO
        INNER JOIN PEDCFG
            ON PEDCAB.PEDIDOCONF = PEDCFG.PEDIDOCONF
            AND PEDCAB.STATUS <> 'C'
        LEFT JOIN ITEMAGRO
            ON PEDITEM.ITEM = ITEMAGRO.ITEM
        LEFT JOIN ITEMMARCA
            ON ITEMAGRO.MARCA = ITEMMARCA.MARCA
        LEFT JOIN CONTAMOV
            ON PEDCAB.PESSOA = CONTAMOV.NUMEROCM
        LEFT JOIN ENDERECO
            ON ENDERECO.NUMEROCM = PEDCAB.PESSOA
            AND ENDERECO.SEQENDERECO = PEDCAB.SEQENDERECO
        LEFT JOIN CIDADE
            ON COALESCE(ENDERECO.CIDADE, CONTAMOV.CIDADE) = CIDADE.CIDADE
        LEFT JOIN PREPRESE
            ON PEDCAB.REPRESENTESTAB = PREPRESE.EMPRESA
            AND PEDCAB.REPRESENT = PREPRESE.REPRESENT
        LEFT JOIN PEMBALAGEM
            ON PEDITEM.EMBALAGEM = PEMBALAGEM.EMBALAGEM
        LEFT JOIN UNIDADE UNIDADE_EMB
            ON UNIDADE_EMB.UNIDADE = PEMBALAGEM.UNDESTINO
        LEFT JOIN UNIDADE UNIDADE_ITE
            ON ITEMAGRO.UNIDADE = UNIDADE_ITE.UNIDADE
        INNER JOIN FILIAL
            ON FILIAL.ESTAB = PEDCAB.ESTAB

        WHERE PEDCAB.SERIE = :SERIE
        AND PEDCAB.NUMERO = :NUMERO
        AND PEDCAB.ESTAB = :ESTAB
    """)

    eng = create_db_engine()
    with eng.connect() as conn:
        rows = conn.execute(sql, {"SERIE": serie, "NUMERO": numero, "ESTAB": estab}).mappings().all()

    if not rows:
        return {}

    def _upper_keys(d):
        return { (k.upper() if isinstance(k, str) else k): v for k, v in d.items() }

    rows = [_upper_keys(r) for r in rows]
    r0 = rows[0]
    header = {
        "ESTAB": r0["ESTAB"],
        "STATUS": r0["STATUS"],
        "ENTRADASAIDA": r0["ENTRADASAIDA"],
        "NUMERO": r0["NUMERO"],
        "DTEMISSAO": r0["DTEMISSAO"],
        "DTVALIDADE": r0["DTVALIDADE"],
        "DTPREVISAO": r0["DTPREVISAO"],
        "SITUACAO": r0["SITUACAO"],
        "NOME": r0["NOME"],
        "CELULAR": r0["CELULAR"],
        "ENDERECO_COMP": r0["ENDERECO_COMP"],
        "VALOR_TOTAL_PEDIDO": r0["VALOR_TOTAL_PEDIDO"],
    }

    items = []
    for r in rows:
        items.append({
            "SEQPEDITE": r["SEQPEDITE"],
            "ITEMDESCRICAO": r["ITEMDESCRICAO"],
            "MARCA": r["MARCA"],
            "QUANTIDADE": r["QUANTIDADE"],
            "UNIDADE": r["UNIDADE"],
            "VALORUNITARIO": r["VALORUNITARIO"],
            "VALOR": r["VALOR"],
        })

    return {"header": header, "items": items}

def compor_mensagem(dados: dict) -> str:
    """
    Monta a mensagem do WhatsApp com cabeçalho + itens.
    Mostra até 5 itens e indica se houver mais.
    """
    if not dados:
        return "Nenhum dado encontrado."

    def fmt_data(dt):
        if isinstance(dt, datetime):
            return dt.strftime("%d/%m/%Y %H:%M")
        return str(dt) if dt is not None else "-"

    def fmt_moeda(v):
        try:
            # Formata como pt-BR simples: 1.234,56
            s = f"{float(v):,.2f}"
            return s.replace(",", "X").replace(".", ",").replace("X", ".")
        except Exception:
            return str(v)

    h = dados["header"]
    itens = dados.get("items", [])

    cabecalho = (
        "📦 Olá *Pedido notificado*\n"
        f"*Número:* {h.get('NUMERO','-')}   *Estab:* {h.get('ESTAB','-')}\n"
        f"*Status:* {h.get('STATUS','-')}   *Situação:* {h.get('SITUACAO','-')}\n"
        f"*Entrada/Saída:* {h.get('ENTRADASAIDA','-')}\n"
        f"*Emissão:* {fmt_data(h.get('DTEMISSAO'))}  "
        f"*Validade:* {fmt_data(h.get('DTVALIDADE'))}  "
        f"*Previsão:* {fmt_data(h.get('DTPREVISAO'))}\n"
        f"*Cliente:* {h.get('NOME','-')}\n"
        f"*Endereço:* {h.get('ENDERECO_COMP','-')}\n"
        f"*Valor total do pedido:* R$ {fmt_moeda(h.get('VALOR_TOTAL_PEDIDO'))}\n"
        "—\n"
        "*Itens:*"
    )

    # Lista até 5 itens
    linhas = []
    max_itens = 5
    for i, it in enumerate(itens[:max_itens], start=1):
        linhas.append(
            f"{i}. {it.get('ITEMDESCRICAO','-')}"
            f"{' ('+it['MARCA']+')' if it.get('MARCA') else ''}\n"
            f"   Qtde: {it.get('QUANTIDADE','-')} {it.get('UNIDADE','-')}  "
            f"Vlr Un.: R$ {fmt_moeda(it.get('VALORUNITARIO'))}  "
            f"Vlr: R$ {fmt_moeda(it.get('VALOR'))}"
        )

    resto = len(itens) - max_itens
    if resto > 0:
        linhas.append(f"... e mais {resto} item(ns).")

    return cabecalho + "\n" + "\n".join(linhas)

def atualizar_status(estab: int, serie: str, numero: int, status: str):
    sql = text("""
        UPDATE CV_PEDCAB_NOTIFICA
           SET STATUS = :status
         WHERE ESTAB = :estab AND SERIE = :serie AND NUMERO = :numero
    """)
    eng = create_db_engine()
    with eng.begin() as conn:
        conn.execute(sql, {"status": status, "estab": estab, "serie": serie, "numero": numero})

def processar_pedidos_pendentes() -> dict:
    """
    Processa todos os pedidos com STATUS='P':
    - Executa run_business_query()
    - Normaliza celular do cliente (CONTAMOV.CELULAR)
    - Se celular for inválido → marca falha + avisa TI
    - Se celular for válido → envia PDF do pedido via Evolution API
    - Atualiza STATUS para 'E' (enviado) ou 'P' (pendente para que ocorra nova tentativa de envio depois da correção do número)
    """

    evo = EvolutionAPI()
    pendentes = fetch_pendentes()

    ok, fail = 0, 0

    for estab, serie, numero in pendentes:
        try:
            # Consulta dados completos do pedido
            dados = run_business_query(estab, serie, numero)
            if not dados:
                raise RuntimeError("Consulta não retornou dados para compor a mensagem.")

            header = dados["header"]

            # ================================
            # 1) Obter celular do cliente
            # ================================
            raw_phone = (header.get("CELULAR") or "").strip()
            phone = normalizar_celular_br(raw_phone)

            if not phone:
                # Celular inválido → marca falha + avisa TI
                atualizar_status(estab, serie, numero, STATUS_PENDENTE)
                print(f"[WARN] Pedido {numero}: celular inválido '{raw_phone}'")

                notificar_ti_pedido_sem_celular(header)
                fail += 1
                continue

            # ================================
            # 2) Enviar PDF do pedido
            # ================================
            enviar_pdf_pedido(dados, phone)

            # Sucesso
            atualizar_status(estab, serie, numero, STATUS_ENVIADO)
            ok += 1

        except Exception as e:
            print(f"[ERRO] {estab}-{serie}-{numero}: {e}")

            try:
                atualizar_status(estab, serie, numero, STATUS_FALHA)
            except Exception:
                pass

            fail += 1

    return {
        "enviados": ok,
        "falhas": fail,
        "total": len(pendentes),
    }

def normalizar_celular_br(raw: str, ddd_default: str = "46") -> str | None:
    """
    Normaliza número de celular brasileiro para o formato:
    55 + DDD (2 dígitos) + número (9 dígitos).

    Regras:
    - Remove tudo que não for número.
    - Se não tiver DDD, usa ddd_default.
    - Garante 9 dígitos no número (se vier com 8, adiciona um '9' na frente).
    - Exemplos:
        99122826        -> 5546999122826
        046999820198    -> 5546999820198
        46  99919321    -> 5546999919321
        05499967796     -> 5554999967796
    """
    if not raw:
        return None

    # só dígitos
    digits = re.sub(r"\D", "", raw)

    # descarta entradas muito curtas
    if len(digits) < 8:
        return None

    # remove zeros à esquerda (ex: 046..., 054...)
    while digits.startswith("0"):
        digits = digits[1:]

    # remove prefixo 55 se já vier
    if digits.startswith("55"):
        digits = digits[2:]

    # se ainda tiver 11+ dígitos, consideramos que já tem DDD
    if len(digits) >= 10:
        ddd = digits[:2]
        local = digits[2:]
    else:
        # não tem DDD, usa o default
        ddd = ddd_default
        local = digits

    # garantir que local tenha 9 dígitos
    if len(local) == 8:
        local = "9" + local
    elif len(local) > 9:
        # mantém os 9 últimos dígitos
        local = local[-9:]
    elif len(local) < 8:
        return None  # muito curto pra ser celular

    # valida DDD com 2 dígitos
    if len(ddd) != 2:
        return None

    return f"55{ddd}{local}"

def notificar_ti_pedido_sem_celular(
    header: dict | None = None,
    *,
    contexto: str | None = None,
    identificador: str | None = None,
    nome_cliente: str | None = None,
    celular_original: str | None = None,
) -> None:
    """
    Envia mensagem para o celular de TI quando o pedido / nota
    não puder ser enviado ao cliente por problema no celular.

    Pode ser chamada de duas formas:

      1) Forma antiga (com header):
         notificar_ti_pedido_sem_celular(header)

      2) Forma nova (contextualizada):
         notificar_ti_pedido_sem_celular(
             contexto="NF-e",
             identificador="SERIE-NRO",
             nome_cliente="Fulano",
             celular_original="(46) 99999-9999"
         )
    """
    ti_phone_raw = os.getenv("TI_NOTIFY_PHONE", "").strip()
    if not ti_phone_raw:
        print("[WARN] TI_NOTIFY_PHONE não definido no .env – não será possível avisar o TI.")
        return

    # Normaliza o celular do TI (se puder)
    ti_phone = normalizar_celular_br(ti_phone_raw) or ti_phone_raw

    # Se veio um header, usamos como fallback para os campos
    if header:
        if identificador is None:
            numero = (header.get("NUMERO") or header.get("numero") or "-")
            serie = (header.get("SERIE") or header.get("serie") or "")
            identificador = f"{serie}-{numero}" if serie else numero

        if nome_cliente is None:
            nome_cliente = header.get("NOME") or header.get("nome") or "Cliente não informado"

        if celular_original is None:
            celular_original = header.get("CELULAR") or header.get("celular") or "-"

    # Defaults caso ainda não tenham sido preenchidos
    contexto = contexto or "Pedido"
    identificador = identificador or "-"
    nome_cliente = nome_cliente or "Cliente não informado"
    celular_original = celular_original or "não informado"

    texto = (
        f"Olá TI! {contexto} {identificador} não foi enviado para {nome_cliente} "
        f"por inconsistências no número do celular ({celular_original}). Verifique!"
    )

    api = EvolutionAPI()
    try:
        api.send_text(phone=ti_phone, text=texto)
        print(
            f"[TI] Aviso enviado para {ti_phone} sobre {contexto} {identificador} "
            f"do cliente {nome_cliente}."
        )
    except Exception as e:
        print(f"[ERRO] Falha ao avisar TI sobre {contexto} {identificador}: {e}")
