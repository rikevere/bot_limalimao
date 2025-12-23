# coopervere/services/pagar_service.py

import os
from datetime import datetime, timedelta, date
from collections import defaultdict
from collections import defaultdict

from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .database import create_db_engine
from .evolution_api import EvolutionAPI
from .pdf_utils import fmt_moeda, fmt_data

load_dotenv()

def get_pay_notify_phones() -> list[str]:
    """
    Lê PAY_NOTIFY_PHONES ou PAY_NOTIFY_PHONE do .env e
    devolve uma lista de números limpos.
    """
    raw = os.getenv("PAY_NOTIFY_PHONES") or os.getenv("PAY_NOTIFY_PHONE", "")
    if not raw:
        return []

    # suporta vírgula ou ponto-e-vírgula
    partes = raw.replace(";", ",").split(",")
    phones = [p.strip() for p in partes if p.strip()]
    return phones


def buscar_contas_pagar(dt_ini: datetime, dt_fim: datetime):
    """
    Executa a query de contas a pagar no intervalo informado.
    Retorna uma lista de dicts (uma linha por duplicata).
    """
    sql = text("""
        SELECT DISTINCT
            PDUPPAGA.FORNECEDOR AS ID_FORNECEDOR,
            PPESSFOR.NOME AS NOME_FORNECEDOR,
            PDUPPAGA.DUPPAG,
            PDUPPAGA.DTEMISSAO,
            PDUPPAGA.DTVENCTO,
            SALDODUP.PNSALDODUPPAG AS SALDO
        FROM PDUPPAGA

LEFT JOIN PPDUPPAG
 ON (PDUPPAGA.EMPRESA         = PPDUPPAG.EMPRESA)
AND (PDUPPAGA.ESTABFORNECEDOR = PPDUPPAG.ESTABFORNECEDOR)
AND (PDUPPAGA.FORNECEDOR      = PPDUPPAG.FORNECEDOR)
AND (PDUPPAGA.DUPPAG          = PPDUPPAG.DUPPAG)

LEFT JOIN AGRFINDUPPAG
ON  (PDUPPAGA.EMPRESA = AGRFINDUPPAG.ESTAB)
AND (PDUPPAGA.DUPPAG = AGRFINDUPPAG.DUPPAG)
AND (PDUPPAGA.ESTABFORNECEDOR = AGRFINDUPPAG.ESTABFORNECEDOR)
AND (PDUPPAGA.FORNECEDOR = AGRFINDUPPAG.FORNECEDOR)

LEFT JOIN NFCABAGRFIN
ON (NFCABAGRFIN.SEQPAGAMENTO = AGRFINDUPPAG.SEQPAGAMENTO)

LEFT JOIN NFCAB
ON (NFCABAGRFIN.ESTAB = NFCAB.ESTAB)
AND (NFCABAGRFIN.SEQNOTA = NFCAB.SEQNOTA)

LEFT JOIN NFITEM
ON (NFITEM.ESTAB = NFCAB.ESTAB)
AND (NFITEM.SEQNOTA = NFCAB.SEQNOTA)   

LEFT JOIN PPORTADO
 ON (PDUPPAGA.EMPRESA  = PPORTADO.EMPRESA)
AND (PDUPPAGA.PORTADOR = PPORTADO.PORTADOR)

LEFT JOIN PPESSFOR
 ON (PDUPPAGA.ESTABFORNECEDOR = PPESSFOR.EMPRESA)
AND (PDUPPAGA.FORNECEDOR      = PPESSFOR.FORNECEDOR)

LEFT JOIN CONTAMOV
 ON (PPESSFOR.FORNECEDOR = CONTAMOV.NUMEROCM)

LEFT JOIN CIDADE
 ON (PPESSFOR.CIDADE = CIDADE.CIDADE)

LEFT JOIN PSITUACA
 ON (PDUPPAGA.SITUACAO = PSITUACA.SITUACAO)

LEFT JOIN PANALITI
 ON (PDUPPAGA.ESTABANALITICA   = PANALITI.EMPRESA)
AND (PDUPPAGA.ANALITICA        = PANALITI.ANALITICA)

LEFT JOIN PSINTETI
 ON (PANALITI.EMPRESA   = PSINTETI.EMPRESA)
AND (PANALITI.SINTETICA = PSINTETI.SINTETICA)

LEFT JOIN PEMPRESA
 ON (PDUPPAGA.EMPRESA = PEMPRESA.EMPRESA)

LEFT JOIN CENCUSCE
 ON (CENCUSCE.CENCUSCOD = PDUPPAGA.CENCUSCOD)
AND (CENCUSCE.CENTROCUS = PDUPPAGA.CENTROCUS)

LEFT JOIN PBAIXASDUPPAG(PDUPPAGA.EMPRESA, PDUPPAGA.ESTABFORNECEDOR,
                        PDUPPAGA.FORNECEDOR, PDUPPAGA.DUPPAG, :DTVENCTOINI) P1
ON (0=0)

LEFT JOIN PORIGEMDADUPPAG(PDUPPAGA.EMPRESA, PDUPPAGA.ESTABFORNECEDOR, PDUPPAGA.FORNECEDOR, PDUPPAGA.DUPPAG) PORIGEMDADUPPAG
ON (0=0)

LEFT JOIN PSALDODUPPAG(PDUPPAGA.EMPRESA, PDUPPAGA.ESTABFORNECEDOR,
                       PDUPPAGA.FORNECEDOR, PDUPPAGA.DUPPAG, 0, CURRENT_DATE + 10000) SALDODUP
ON (0=0)  

LEFT JOIN RATCC
 ON (RATCC.ESTAB = PDUPPAGA.EMPRESA)
AND (RATCC.ESTABFORNECEDOR = PDUPPAGA.ESTABFORNECEDOR)
AND (RATCC.PESSOA = PDUPPAGA.FORNECEDOR)
AND (RATCC.DUPPAG = PDUPPAGA.DUPPAG)

LEFT JOIN SALDOAUTDUPPAG(PDUPPAGA.EMPRESA, PDUPPAGA.ESTABFORNECEDOR,
                         PDUPPAGA.FORNECEDOR, PDUPPAGA.DUPPAG) SALDOAUTPAG
ON 0=0 

 WHERE (PDUPPAGA.EMPRESA = 1) AND (PANALITI.ATIVA = 'S') AND
 (PDUPPAGA.DTVENCTO between :DTVENCTOINI and :DTVENCTOFIM) AND ((PDUPPAGA.QUITADA = 'N') OR (PDUPPAGA.QUITADA IS NULL)) AND ((CONTAMOV.MATFUNCIONARIO = '') OR (CONTAMOV.MATFUNCIONARIO IS NULL))
    """)

    eng = create_db_engine()
    with eng.connect() as conn:
        rows = conn.execute(sql, {
            "DTVENCTOINI": dt_ini.strftime("%Y-%m-%d"),
            "DTVENCTOFIM": dt_fim.strftime("%Y-%m-%d"),
        }).mappings().all()

    return [dict(r) for r in rows]


def agrupar_por_fornecedor_e_data(linhas):
    """
    Agora estrutura o resultado como:
    {
        date(2025,12, 5): {
            "Fornecedor X (1)": 5925.00,
            "Fornecedor Y (...)": 7325.00
        },
        date(2025,12, 3): {
            "Fornecedor Z (...)": 1000.00
        },
        ...
    }
    Ou seja: primeiro por data, depois por fornecedor.
    """
    agrupado = defaultdict(lambda: defaultdict(float))

    for r in linhas:
        # nomes em minúsculo, conforme debug:
        # {'id_fornecedor': ..., 'nome_fornecedor': ..., 'dtvencto': ..., 'saldo': ...}
        fornecedor = r.get("nome_fornecedor") or "Fornecedor não informado"

        dt_key = parse_date(r.get("dtvencto"))
        if not dt_key:
            # pula registros com data inválida
            continue

        valor = float(r.get("saldo") or 0)
        agrupado[dt_key][fornecedor] += valor

    return agrupado


def montar_mensagem_contas(agrupado: dict, dias: int, dt_ini: datetime, dt_fim: datetime) -> str:
    """
    Gera uma mensagem amigável para envio via WhatsApp.

    - Ordena por data de vencimento (do mais recente para o mais antigo)
    - Dentro de cada data, lista os fornecedores com seu respectivo valor.
    """

    # Garante formatação correta mesmo se dt_ini/dt_fim forem datetime
    def _fmt_dt(x):
        if isinstance(x, datetime):
            return fmt_data(x.date())
        return fmt_data(x)

    msg = []
    msg.append(
        f"Olá, Gestor! Aqui estão os compromissos da Cooperverê\n"
        f"para os próximos *{dias} dias*\n"
        f"(*{_fmt_dt(dt_ini)}* a *{_fmt_dt(dt_fim)}*).\n"
    )

    if not agrupado:
        msg.append("\nNão há compromissos previstos neste período.")
        return "\n".join(msg)

    # datas em ordem do vencimento mais próximo (mais antigo) para o mais distante
    for dt in sorted(agrupado.keys()):
        msg.append(f"\n*Vencimento:* {fmt_data(dt)}")
        for fornecedor, valor in sorted(agrupado[dt].items(), key=lambda x: x[0]):
            msg.append(f"   - {fornecedor} – R$ {fmt_moeda(valor)}")

    return "\n".join(msg)


def processar_contas_pagar() -> dict:
    """
    Função principal.
    Lê parâmetros do .env, consulta as duplicatas, gera texto e envia pelo Evolution API
    para todos os telefones configurados em:
      - PAY_NOTIFY_PHONES (lista separada por vírgula ou ;)
      - ou PAY_NOTIFY_PHONE (único número).
    """

    phones = get_pay_notify_phones()
    if not phones:
        raise RuntimeError("PAY_NOTIFY_PHONES ou PAY_NOTIFY_PHONE não definido no .env")

    dias = int(os.getenv("PAY_REPORT_RANGE_DAYS", "7"))
    offset = int(os.getenv("PAY_REPORT_START_OFFSET_DAYS", "0"))

    dt_ini = datetime.now().date() + timedelta(days=offset)
    dt_fim = dt_ini + timedelta(days=dias - 1)

    linhas = buscar_contas_pagar(dt_ini, dt_fim)
    agrupado = agrupar_por_fornecedor_e_data(linhas)
    mensagem = montar_mensagem_contas(agrupado, dias, dt_ini, dt_fim)

    evo = EvolutionAPI()

    for phone in phones:
        try:
            evo.send_text(phone, mensagem)
            print(f"[Payables] Relatório enviado para {phone}")
        except Exception as e:
            print(f"[Payables][ERRO] ao enviar para {phone}: {e}")

    #print("\n=== DEBUG CAMPOS DA QUERY ===")
    #print("Primeira linha:", linhas[0] if linhas else "Nenhuma")

    return {
        "intervalo_inicio": str(dt_ini),
        "intervalo_fim": str(dt_fim),
        "fornecedores": len(agrupado),
        "total_linhas": len(linhas),
        "destinatarios": phones,
    }



def parse_date(dt):
    """
    Converte o valor vindo do banco (date, datetime ou string)
    em um objeto date. Se não conseguir, retorna None.
    """
    if dt is None:
        return None

    # Se já for date ou datetime
    if isinstance(dt, (date, datetime)):
        return dt if isinstance(dt, date) and not isinstance(dt, datetime) else dt.date()

    # Se for string
    if isinstance(dt, str):
        s = dt.strip()
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue

    # Não reconheceu
    print("[AVISO] Data não reconhecida em parse_date:", dt, type(dt))
    return None
