import os
from datetime import date
from typing import Dict, List

from dotenv import load_dotenv
from sqlalchemy import text

from .database import create_db_engine
from .evolution_api import EvolutionAPI
from .notifier_service import normalizar_celular_br, notificar_ti_pedido_sem_celular
from .state_manager import load_aniversarios_enviados, save_aniversarios_enviados


load_dotenv()


BIRTHDAY_SQL = text(
    """
 SELECT
    P.cli_codigo AS CLIENTE,
    P.cli_nome AS NOME,
    P.cli_datanascimento AS ANIVERSARIO,
    P.cli_telefone AS CELULAR,
    'N' AS ASSOCIADO
FROM clientes P
WHERE P.cli_status = 'Ativo'
  AND P.cli_datanascimento IS NOT NULL
  AND MONTH(P.cli_datanascimento) = :mes
  AND DAY(P.cli_datanascimento) = :dia
    """
)


def _eh_associado(valor: str | None) -> bool:
    return (valor or "").strip().upper() == "S"


def _montar_mensagem(nome: str, associado: bool) -> str:
    primeiro_nome = (nome or "").split(" ")[0].strip() or "Cliente"

    if associado:
        return (
        f"🎉 Feliz aniversário, {primeiro_nome}!\n"
        "A Lima Limão deseja a você um dia iluminado e cheio de alegria. "
        "Que não falte disposição para os seus treinos e leveza para os seus momentos de lazer. "
        "Obrigada por fazer parte da nossa história e levar nosso estilo com você!"
    )

    return (
        f"🎉 Feliz aniversário, {primeiro_nome}!\n"
        "A Lima Limão deseja a você um dia iluminado e cheio de alegria. "
        "Que não falte disposição para os seus treinos e leveza para os seus momentos de lazer. "
        "Obrigada por fazer parte da nossa história e levar nosso estilo com você!"
    )


def buscar_aniversariantes(hoje: date) -> List[Dict]:
    eng = create_db_engine()
    with eng.connect() as conn:
        rows = conn.execute(BIRTHDAY_SQL, {"mes": hoje.month, "dia": hoje.day}).mappings().all()

    return [dict(r) for r in rows]


def processar_aniversariantes(data_referencia: date | None = None) -> dict:
    """Envia felicitações de aniversário via WhatsApp somente no dia do aniversário.

    A função pode ser executada diversas vezes ao dia, mas um cliente só recebe
    nova mensagem caso ainda não tenha recebido no mesmo dia. Caso o número
    esteja inconsistente, nenhuma marcação é feita para permitir novas
    tentativas após a correção.
    """

    hoje = data_referencia or date.today()
    enviados_por_cliente = load_aniversarios_enviados()
    evo = EvolutionAPI()

    stats = {
        "total": 0,
        "enviados": 0,
        "ja_enviados": 0,
        "sem_celular": 0,
        "falhas": 0,
    }

    aniversariantes = buscar_aniversariantes(hoje)
    stats["total"] = len(aniversariantes)

    for linha in aniversariantes:
        cliente_id = str(linha.get("CLIENTE") or linha.get("cliente") or "").strip()
        nome = linha.get("NOME") or linha.get("nome") or "Cliente"
        associado = _eh_associado(linha.get("associado") or linha.get("ASSOCIADO"))

        if not cliente_id:
            continue

        if enviados_por_cliente.get(cliente_id) == hoje.isoformat():
            stats["ja_enviados"] += 1
            continue

        telefone_raw = (linha.get("CELULAR") or linha.get("celular") or "").strip()
        #telefone_raw = '46999111465' # telefone teste
        telefone = normalizar_celular_br(telefone_raw)

        if not telefone:
            stats["sem_celular"] += 1
            notificar_ti_pedido_sem_celular(
                contexto="Aniversariante",
                identificador=cliente_id,
                nome_cliente=nome,
                celular_original=telefone_raw,
            )
            continue

        mensagem = _montar_mensagem(nome, associado)

        try:
            evo.send_text(telefone, mensagem)
            enviados_por_cliente[cliente_id] = hoje.isoformat()
            save_aniversarios_enviados(enviados_por_cliente)
            stats["enviados"] += 1
        except Exception as e:
            stats["falhas"] += 1
            print(f"[Aniversarios][ERRO] Falha ao enviar para {cliente_id} ({telefone}): {e}")

    return stats