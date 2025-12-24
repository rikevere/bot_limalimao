# services/cobranca_service.py
from __future__ import annotations

import os
import re
import logging
from dataclasses import dataclass
from datetime import date, timedelta, datetime
from typing import Any, Dict, List, Tuple, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine
from .notifier_service import normalizar_celular_br

from .database import create_db_engine
from .evolution_api import EvolutionAPI, EvolutionAPIError


# -----------------------------------------------------------------------------
# Configurações (.env)
# -----------------------------------------------------------------------------
TI_WHATSAPP_NUMBER = os.getenv("TI_NOTIFY_PHONE", "").strip()   # ex.: 5541999999999
COBRANCA_HORARIO_INICIO = os.getenv("COBRANCA_HORARIO_INICIO", "09:00").strip()
COBRANCA_HORARIO_FIM = os.getenv("COBRANCA_HORARIO_FIM", "17:59").strip()

# Categoria interna (apenas para deduplicar alertas ao TI)
TI_DEDUP_CATEGORIA = "TI_TELEFONE_INVALIDO"


# -----------------------------------------------------------------------------
# Categorias vigentes
# -----------------------------------------------------------------------------
def gerar_categorias() -> List[Tuple[str, Any]]:
    hoje = date.today()
    return [
        ("vence_hoje", hoje),

        # Exemplos (se quiser reativar depois):
        # ("a_vencer_10_dias", hoje + timedelta(days=10)),
        # ("a_vencer_5_dias", hoje + timedelta(days=5)),
        # ("vencida_5_dias", hoje - timedelta(days=5)),
        # ("vencida_10_dias", hoje - timedelta(days=10)),
        # ("vencida_mais_30_dias", (hoje - timedelta(days=9999), hoje - timedelta(days=30))),
    ]


# -----------------------------------------------------------------------------
# SQL base (a consulta que você definiu como correta)
# -----------------------------------------------------------------------------
COBRANCA_SQL_BASE = """
SELECT
    fm.mfi_codigo,
    fm.mfi_data_vencimento,
    fm.mfi_data_recebimento,
    fm.mfi_valor,
    fm.mfi_cliente,
    c.cli_codigo,
    c.cli_nome,
    c.cli_telefone AS telefone
FROM financeiro_mov fm
LEFT JOIN clientes c ON c.cli_codigo = fm.mfi_cliente
WHERE
(
  (fm.mfi_data_recebimento IS NULL AND fm.mfi_data_vencimento BETWEEN :DATA_INICIAL AND :DATA_FINAL)
  OR
  (fm.mfi_data_recebimento IS NOT NULL AND fm.mfi_data_recebimento BETWEEN :DATA_INICIAL AND :DATA_FINAL)
)
AND fm.mfi_operacao = 'E'
AND fm.mfi_status = 'P'
AND (fm.mfi_proc IS NULL OR fm.mfi_proc NOT IN ('E','C'))
AND NOT EXISTS (
    SELECT 1
    FROM financeiro_mov_groups g
    WHERE g.fmb_agrupado = fm.mfi_codigo
)
{FILTRO_LOG}
ORDER BY fm.mfi_data_vencimento
"""


# -----------------------------------------------------------------------------
# Model
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class CobrancaItem:
    mfi_codigo: str               # VARCHAR(12)
    cli_codigo: str               # geralmente VARCHAR (mantemos string)
    cli_nome: str
    telefone: Optional[str]
    data_vencimento: Optional[date]
    data_recebimento: Optional[date]
    valor: Optional[float]


# -----------------------------------------------------------------------------
# Utilitários
# -----------------------------------------------------------------------------
def _parse_hhmm(s: str) -> Tuple[int, int]:
    m = re.match(r"^(\d{2}):(\d{2})$", s or "")
    if not m:
        return (9, 0)
    return int(m.group(1)), int(m.group(2))


def _is_within_business_hours(now: datetime) -> bool:
    h1, m1 = _parse_hhmm(COBRANCA_HORARIO_INICIO)
    h2, m2 = _parse_hhmm(COBRANCA_HORARIO_FIM)
    start = now.replace(hour=h1, minute=m1, second=0, microsecond=0)
    end = now.replace(hour=h2, minute=m2, second=59, microsecond=999999)
    return start <= now <= end


def _format_brl(value: Optional[float]) -> str:
    if not isinstance(value, (int, float)):
        return "—"
    s = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


# -----------------------------------------------------------------------------
# FILTRO_LOG e LOG de envios
# -----------------------------------------------------------------------------
def _build_filtro_log(categoria: str) -> str:
    """
    Regra:
    - Para as categorias normais: não reenviar se já existe OK para a mesma cobrança/categoria.
    - Para "vencida_mais_30_dias": não reenviar se OK nos últimos 5 dias.
    """
    if categoria == "vencida_mais_30_dias":
        return """
AND NOT EXISTS (
    SELECT 1
      FROM log_envio_whatsapp l
     WHERE l.id_cobranca = fm.mfi_codigo
       AND l.categoria = :CATEGORIA
       AND l.status_envio = 'OK'
       AND DATEDIFF(CURDATE(), l.data_envio) < 5
)
"""
    return """
AND NOT EXISTS (
    SELECT 1
      FROM log_envio_whatsapp l
     WHERE l.id_cobranca = fm.mfi_codigo
       AND l.categoria = :CATEGORIA
       AND l.status_envio = 'OK'
)
"""


def registrar_envio(
    eng: Engine,
    *,
    id_cobranca: str,
    categoria: str,
    status_envio: str,
    mensagem_erro: Optional[str] = None,
) -> None:
    """
    Grava um log de envio.
    - status_envio: 'OK' ou 'ERRO'
    - mensagem_erro: texto livre quando ERRO (ou quando TI foi notificado)
    """
    sql = text("""
        INSERT INTO log_envio_whatsapp (id_cobranca, categoria, status_envio, mensagem_erro, data_envio)
        VALUES (:id_cobranca, :categoria, :status_envio, :mensagem_erro, NOW())
    """)
    with eng.begin() as conn:
        conn.execute(
            sql,
            {
                "id_cobranca": id_cobranca,
                "categoria": categoria,
                "status_envio": status_envio,
                "mensagem_erro": mensagem_erro,
            },
        )


def ti_ja_notificado(eng: Engine, *, id_cobranca: str) -> bool:
    sql = text("""
        SELECT 1
          FROM log_envio_whatsapp
         WHERE id_cobranca = :id
           AND categoria = :cat
           AND status_envio = 'OK'
         LIMIT 1
    """)
    with eng.connect() as conn:
        return conn.execute(sql, {"id": id_cobranca, "cat": TI_DEDUP_CATEGORIA}).first() is not None


# -----------------------------------------------------------------------------
# Consulta
# -----------------------------------------------------------------------------
def buscar_cobrancas_por_categoria(
    eng: Engine,
    *,
    data_inicial: date,
    data_final: date,
    categoria: str,
    filtro_log_extra_sql: str = "",
) -> List[CobrancaItem]:
    """
    Retorna cobranças para a categoria/datas já aplicando o bloqueio via log (OK).
    - filtro_log_extra_sql: se quiser injetar mais restrições além do filtro padrão.
    """
    filtro_log = _build_filtro_log(categoria)
    filtro_log = (filtro_log + "\n" + (filtro_log_extra_sql or "")).strip()

    sql = text(COBRANCA_SQL_BASE.format(FILTRO_LOG=("\n" + filtro_log if filtro_log else "")))

    with eng.connect() as conn:
        rows = conn.execute(
            sql,
            {
                "DATA_INICIAL": data_inicial,
                "DATA_FINAL": data_final,
                "CATEGORIA": categoria,
            },
        ).mappings().all()

    itens: List[CobrancaItem] = []
    for r in rows:
        itens.append(
            CobrancaItem(
                mfi_codigo=str(r["mfi_codigo"]),
                cli_codigo=str(r["cli_codigo"]),
                cli_nome=str(r["cli_nome"] or "").strip(),
                telefone=(r.get("telefone") or None),
                data_vencimento=r.get("mfi_data_vencimento"),
                data_recebimento=r.get("mfi_data_recebimento"),
                valor=r.get("mfi_valor"),
            )
        )
    return itens


# -----------------------------------------------------------------------------
# Templates de mensagens
# -----------------------------------------------------------------------------
def montar_mensagem_cliente(cli_nome: str, categoria: str, itens: List[CobrancaItem]) -> str:
    if categoria == "vence_hoje":
        titulo = "📌 Passando para lembrar você sobre um vencimento de hoje"
    elif categoria.startswith("a_vencer"):
        titulo = "📌 Passando para lembrar você sobre um próximo vencimento"
    else:
        titulo = "⚠️ Aviso importante"

    linhas: List[str] = []
    for it in itens:
        dt_txt = it.data_vencimento.strftime("%d/%m/%Y") if it.data_vencimento else "—"
        vl_txt = _format_brl(it.valor)
        linhas.append(f"• Duplicata {it.mfi_codigo} — vencimento {dt_txt} — valor {vl_txt}")

    corpo = "\n".join(linhas) if linhas else "• (sem itens no momento)"

    return (
        f"Olá, {cli_nome}! 😊\n\n"
        f"{titulo}.\n\n"
        f"{corpo}\n\n"
        "Caso o pagamento já tenha sido realizado, por favor desconsidere esta mensagem.\n"
        "Se precisar de algo ou tiver qualquer dúvida, estamos à disposição. 🤝"
    )


def montar_mensagem_ti(cli_codigo: str, cli_nome: str, telefone_raw: Optional[str], itens: List[CobrancaItem]) -> str:
    ids = ", ".join(i.mfi_codigo for i in itens)
    return (
        "⚠️ Telefone inválido para notificação de cobrança\n\n"
        f"Cliente: {cli_nome} (cód. {cli_codigo})\n"
        f"Telefone no cadastro: {telefone_raw or 'VAZIO'}\n"
        f"Duplicatas pendentes (mfi_codigo): {ids}\n\n"
        "Ajustar telefone no cadastro para liberar envio."
    )


# -----------------------------------------------------------------------------
# Serviço principal
# -----------------------------------------------------------------------------
def processar_cobrancas() -> Dict[str, Any]:
    """
    1) Consulta as duplicatas nas categorias vigentes.
    2) Envia mensagem por cliente/categoria (1 mensagem agrupada).
    3) Garante idempotência: só bloqueia reenvio quando existir log_envio_whatsapp OK.
    4) Se telefone inválido: notifica TI (dedup) e NÃO grava OK na categoria -> permanece pendente.
    """
    agora = datetime.now()
    if not _is_within_business_hours(agora):
        return {"ok": True, "skipped": True, "reason": f"fora do horário ({agora:%H:%M})"}

    eng = create_db_engine()
    api = EvolutionAPI()

    total_itens = 0
    total_ok = 0
    total_clientes = 0
    total_invalidos = 0
    total_erros_envio = 0
    total_ti_notificados = 0

    for cat in gerar_categorias():
        categoria = cat[0]

        # resolução do range
        if isinstance(cat[1], tuple):
            data_inicial, data_final = cat[1]
        elif len(cat) == 3:
            data_inicial = cat[1]
            data_final = cat[2]
        else:
            data_inicial = data_final = cat[1]

        itens = buscar_cobrancas_por_categoria(
            eng,
            data_inicial=data_inicial,
            data_final=data_final,
            categoria=categoria,
        )
        total_itens += len(itens)

        # Agrupa por cliente para enviar 1 mensagem por cliente/categoria
        grupos: Dict[Tuple[str, str, Optional[str]], List[CobrancaItem]] = {}
        for it in itens:
            key = (it.cli_codigo, it.cli_nome, it.telefone)
            grupos.setdefault(key, []).append(it)

        logging.info(f"[Cobrança] Categoria={categoria} => {len(itens)} itens | {len(grupos)} cliente(s)")

        for (cli_codigo, cli_nome, telefone_raw), itens_cli in grupos.items():
            total_clientes += 1

            #telefone_norm = normalizar_celular_br('46999111465')
            telefone_norm = normalizar_celular_br(telefone_raw)

            # Telefone inválido -> avisa TI (1x por duplicata) e mantém pendente (não grava OK da categoria)
            if not telefone_norm:
                total_invalidos += 1
                logging.warning(f"[Cobrança] Telefone inválido | cli={cli_codigo} {cli_nome} | raw={telefone_raw}")

                if TI_WHATSAPP_NUMBER:
                    pendentes_ti = [i for i in itens_cli if not ti_ja_notificado(eng, id_cobranca=i.mfi_codigo)]
                    if pendentes_ti:
                        msg_ti = montar_mensagem_ti(cli_codigo, cli_nome, telefone_raw, pendentes_ti)
                        try:
                            api.send_text(TI_WHATSAPP_NUMBER, msg_ti)
                            total_ti_notificados += 1
                            for i in pendentes_ti:
                                registrar_envio(
                                    eng,
                                    id_cobranca=i.mfi_codigo,
                                    categoria=TI_DEDUP_CATEGORIA,
                                    status_envio="OK",
                                    mensagem_erro="Telefone inválido no cadastro (aviso ao TI)",
                                )
                        except Exception as e:
                            logging.error(f"[Cobrança][TI] Falha ao notificar TI: {e}")
                continue

            # Envia 1 mensagem por cliente/categoria
            msg = montar_mensagem_cliente(cli_nome, categoria, itens_cli)
            try:
                api.send_text(telefone_norm, msg)

                # Marca OK para cada duplicata da mensagem
                for it in itens_cli:
                    registrar_envio(
                        eng,
                        id_cobranca=it.mfi_codigo,
                        categoria=categoria,
                        status_envio="OK",
                        mensagem_erro=None,
                    )
                    total_ok += 1

                logging.info(
                    f"[Cobrança] OK => {cli_nome} ({telefone_norm}) | categoria={categoria} | itens={len(itens_cli)}"
                )

            except (EvolutionAPIError, Exception) as e:
                total_erros_envio += 1
                logging.error(
                    f"[Cobrança] ERRO envio => {cli_nome} ({telefone_norm}) | categoria={categoria} | {e}"
                )
                # Opcional: registrar erro (mas NÃO bloqueia reenvio)
                for it in itens_cli:
                    try:
                        registrar_envio(
                            eng,
                            id_cobranca=it.mfi_codigo,
                            categoria=categoria,
                            status_envio="ERRO",
                            mensagem_erro=str(e),
                        )
                    except Exception:
                        pass

    return {
        "ok": True,
        "skipped": False,
        "itens_consultados": total_itens,
        "cobrancas_marcadas_ok": total_ok,          # qtde de duplicatas marcadas OK
        "clientes_processados": total_clientes,
        "telefones_invalidos": total_invalidos,
        "ti_notificados": total_ti_notificados,
        "erros_envio": total_erros_envio,
    }
