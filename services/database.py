# services/database.py
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from urllib.parse import quote_plus # Adicione este import no topo do arquivo

load_dotenv()

# Escolha um dos DRIVERS: "firebird" (moderno) ou "fdb" (legado)
DBAPI_DRIVER = os.getenv("DBAPI_DRIVER", "firebird")  # "firebird" ou "fdb"

def normalize_windows_path(p: str) -> str:
    # SQLAlchemy/Firebird preferem barras normais
    return p.replace("\\", "/")

def get_database_url() -> str:
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    host = os.getenv("MYSQL_HOST", "localhost")
    port = os.getenv("MYSQL_PORT", "3306")
    db = os.getenv("MYSQL_DB")

    # Protege caracteres especiais no usuário e senha
    user_safe = quote_plus(user) if user else ""
    pass_safe = quote_plus(password) if password else ""

    return f"mysql+pymysql://{user_safe}:{pass_safe}@{host}:{port}/{db}"

def create_db_engine() -> Engine:
    db_url = get_database_url()
    engine = create_engine(db_url, pool_pre_ping=True, future=True)
    return engine

def test_connection():
    engine = create_db_engine()
    with engine.connect() as conn:
        ts = conn.execute(text("SELECT CURRENT_TIMESTAMP")).scalar_one()
        print("Conexão OK:", ts)

if __name__ == "__main__":
    test_connection()

