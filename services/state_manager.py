# services/state_manager.py
import os
from datetime import date
from typing import Optional
import json

# Caminho do arquivo de estado.
# __file__ -> services/state_manager.py
# dirname(__file__) -> .../services
# dirname(dirname(__file__)) -> raiz do projeto (coopervere)
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
STATE_DIR = os.path.join(PROJECT_ROOT, "state")
STATE_FILE_SEMANA = os.path.join(STATE_DIR, "ultima_execucao_semana.txt")
STATE_FILE_ANIVERSARIOS = os.path.join(STATE_DIR, "aniversarios_enviados.json")
STATE_FILE_FESTIVIDADES = os.path.join(STATE_DIR, "festividades_enviados.json")


def _ensure_state_dir():
    """Garante que a pasta de estado exista."""
    os.makedirs(STATE_DIR, exist_ok=True)


def load_ultima_execucao_semana() -> Optional[date]:
    """
    Lê do disco a data da última execução semanal.

    Retorna:
        - date: se o arquivo existir e a data for válida (formato YYYY-MM-DD)
        - None: se o arquivo não existir ou houver erro de leitura/parse
    """
    try:
        if not os.path.exists(STATE_FILE_SEMANA):
            return None

        with open(STATE_FILE_SEMANA, "r", encoding="utf-8") as f:
            conteudo = f.read().strip()

        if not conteudo:
            return None

        # Formato ISO: YYYY-MM-DD
        return date.fromisoformat(conteudo)
    except Exception:
        # Em caso de erro qualquer, consideramos como "nunca executado"
        return None


def save_ultima_execucao_semana(d: date) -> None:
    """
    Salva no disco a data da última execução semanal.

    Args:
        d (date): data que será gravada no arquivo em formato YYYY-MM-DD.
    """
    _ensure_state_dir()
    try:
        with open(STATE_FILE_SEMANA, "w", encoding="utf-8") as f:
            f.write(d.isoformat())
    except Exception as e:
        # Não vamos explodir a aplicação por erro de gravação,
        # mas é bom logar algo no console.
        print(f"[state_manager] Erro ao salvar ultima_execucao_semana: {e}")


def load_aniversarios_enviados() -> dict:
    """Carrega do disco o mapa de aniversários já notificados.

    O formato esperado é um dicionário simples: {"<cliente>": "YYYY-MM-DD"},
    indicando a última data em que a mensagem foi enviada com sucesso.
    Caso o arquivo não exista ou esteja inválido, devolve um dicionário vazio.
    """
    try:
        if not os.path.exists(STATE_FILE_ANIVERSARIOS):
            return {}

        with open(STATE_FILE_ANIVERSARIOS, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict):
            # garante que as chaves sejam strings; mantém valor como string se possível
            return {str(k): (str(v) if v is not None else "") for k, v in data.items()}
    except Exception as e:
        print(f"[state_manager] Erro ao ler aniversarios_enviados: {e}")

    return {}


def save_aniversarios_enviados(data: dict) -> None:
    """Persiste o mapa de aniversários enviados no disco.

    Args:
        data: dicionário no formato {"<cliente>": "YYYY-MM-DD"}.
    """
    _ensure_state_dir()
    try:
        with open(STATE_FILE_ANIVERSARIOS, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[state_manager] Erro ao salvar aniversarios_enviados: {e}")

def load_festividades_enviados() -> dict:
    """Carrega do disco o mapa de envios de festividades (Natal/Ano Novo).

    Formato esperado:
    {
      "<cliente>": {
        "natal": "YYYY-MM-DD",
        "ano_novo": "YYYY-MM-DD"
      }
    }
    """
    try:
        if not os.path.exists(STATE_FILE_FESTIVIDADES):
            return {}

        with open(STATE_FILE_FESTIVIDADES, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict):
            out = {}
            for k, v in data.items():
                if isinstance(v, dict):
                    out[str(k)] = {str(subk): (str(subv) if subv is not None else "") for subk, subv in v.items()}
            return out
    except Exception as e:
        print(f"[state_manager] Erro ao ler festividades_enviados: {e}")

    return {}


def save_festividades_enviados(data: dict) -> None:
    """Persiste o mapa de envios de festividades (Natal/Ano Novo) no disco."""
    _ensure_state_dir()
    try:
        with open(STATE_FILE_FESTIVIDADES, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[state_manager] Erro ao salvar festividades_enviados: {e}")        