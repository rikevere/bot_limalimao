import time
from datetime import date
from typing import Dict, List, Literal

from dotenv import load_dotenv
from sqlalchemy import text

from .database import create_db_engine
from .evolution_api import EvolutionAPI
from .notifier_service import normalizar_celular_br
from .state_manager import load_festividades_enviados, save_festividades_enviados


load_dotenv()


FESTIVIDADES_SQL = text(
    """
 SELECT
    cli_codigo AS CLIENTE,
    cli_nome AS NOME,
    cli_telefone AS CELULAR
 FROM clientes
 WHERE clientes.cli_status = 'Ativo'
 AND clientes.cli_telefone IS NOT NULL 
 AND clientes.cli_telefone != ''
    """
)


def _primeiro_nome(nome: str | None) -> str:
    return (nome or "").split(" ")[0].strip() or "Cliente"


def _montar_mensagem_festividade(tipo: Literal["natal", "ano_novo"], nome: str, ano_base: int) -> str:
    primeiro_nome = _primeiro_nome(nome)

    if tipo == "natal":
        return (
            f"🎄 Olá, {primeiro_nome}!\n\n"
            "A Lima Limão agradece por você fazer parte da nossa história este ano. "
            "Que o seu Natal seja repleto de brilho, conforto e momentos especiais ao lado de quem você ama. "
            "Esperamos que você celebre com muito estilo e energia. Boas festas!"
        )

    proximo_ano = ano_base + 1
    return (
        f"✨ Olá, {primeiro_nome}!\n\n"
        "A Lima Limão deseja que o seu Ano Novo chegue com muita cor, leveza e movimento. "
        f"Obrigado por nos escolher para acompanhar seus treinos e seus momentos de lazer em {ano_base}. "
        f"Que em {proximo_ano} possamos alcançar novas metas e conquistas juntos. Feliz Ano Novo!"
    )


def buscar_contatos_festividade() -> List[Dict]:
    eng = create_db_engine()
    with eng.connect() as conn:
        rows = conn.execute(FESTIVIDADES_SQL).mappings().all()

    return [dict(r) for r in rows]


def processar_festividades(
    tipo: Literal["natal", "ano_novo"],
    data_referencia: date | None = None,
) -> dict:
    """Envia mensagens de Natal/Ano Novo para contatos com telefone válido.

    Executa apenas em 24/12 (natal) e 31/12 (ano novo) e registra envios por data,
    evitando duplicidade no mesmo dia/ano. Entre cada envio, aguarda 10s para
    reduzir risco de bloqueio por spam.
    """
    if tipo not in {"natal", "ano_novo"}:
        raise ValueError("Tipo de festividade inválido. Use 'natal' ou 'ano_novo'.")

    hoje = data_referencia or date.today()

    if tipo == "natal" and (hoje.month != 12 or hoje.day != 24):
        return {"erro": "Fora da data de Natal"}
    if tipo == "ano_novo" and (hoje.month != 12 or hoje.day != 31):
        return {"erro": "Fora da data de Ano Novo"}

    enviados_por_cliente = load_festividades_enviados()
    evo = EvolutionAPI()

    stats = {
        "total": 0,
        "enviados": 0,
        "ja_enviados": 0,
        "sem_celular": 0,
        "falhas": 0,
    }

    contatos = buscar_contatos_festividade()
    stats["total"] = len(contatos)

    for linha in contatos:
        cliente_id = str(linha.get("CLIENTE") or linha.get("cliente") or "").strip()
        nome = linha.get("NOME") or linha.get("nome") or "Cliente"

        if not cliente_id:
            continue

        registro_cliente = enviados_por_cliente.get(cliente_id, {})
        if isinstance(registro_cliente, dict):
            if registro_cliente.get(tipo) == hoje.isoformat():
                stats["ja_enviados"] += 1
                continue
        else:
            registro_cliente = {}

        telefone_raw = (linha.get("CELULAR") or linha.get("celular") or "").strip()
        #telefone_raw = '46999111465'
        telefone = normalizar_celular_br(telefone_raw)

        if not telefone:
            stats["sem_celular"] += 1
            continue

        mensagem = _montar_mensagem_festividade(tipo, nome, hoje.year)
        envio_tentado = False

        try:
            evo.send_text(telefone, mensagem)
            registro_cliente[tipo] = hoje.isoformat()
            enviados_por_cliente[cliente_id] = registro_cliente
            save_festividades_enviados(enviados_por_cliente)
            stats["enviados"] += 1
            envio_tentado = True
        except Exception as e:
            stats["falhas"] += 1
            print(f"[Festividades][ERRO] Falha ao enviar para {cliente_id} ({telefone}): {e}")
            envio_tentado = True
        finally:
            if envio_tentado:
                time.sleep(10)

    return stats