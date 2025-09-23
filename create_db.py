from sqlalchemy import text
from config import engine

# Conexão para criar o banco

with engine.connect() as conn:
    conn.execute(text("COMMIT"))  # necessário antes do CREATE DATABASE
    conn.execute(text("CREATE DATABASE winfut;"))
    conn.commit()

# Agora conecta ao banco recém-criado
tables_queries = [
    """
    CREATE TABLE IF NOT EXISTS all_time_ticks(
        id SERIAL PRIMARY KEY,
        Ativo TEXT,
        Data DATE,
        Hora TIME,
        Comprador TEXT,
        Preco NUMERIC,
        Quantidade NUMERIC,
        Vendedor TEXT,
        Tipo TEXT
    );
    """,
    """
    
    CREATE TABLE IF NOT EXISTS WINFUT_8R(
        id SERIAL PRIMARY KEY,
        Ativo TEXT,
        Data DATE,
        Hora TIME,
        Abertura NUMERIC,
        Fechamento NUMERIC,
        Maxima NUMERIC,
        Minima NUMERIC,
        Volume NUMERIC,
        Negocios INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS WINFUT_13R(
        id SERIAL PRIMARY KEY,
        Ativo TEXT,
        Data DATE,
        Hora TIME,
        Abertura NUMERIC,
        Fechamento NUMERIC,
        Maxima NUMERIC,
        Minima NUMERIC,
        Volume NUMERIC,
        Negocios INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS WINFUT_40R(
        id SERIAL PRIMARY KEY,
        Ativo TEXT,
        Data DATE,
        Hora TIME,
        Abertura NUMERIC,
        Fechamento NUMERIC,
        Maxima NUMERIC,
        Minima NUMERIC,
        Volume NUMERIC,
        Negocios INTEGER
    );
    """
]

with engine.connect() as conn:
    for query in tables_queries:
        conn.execute(text(query))
        conn.commit()

print("Todas as tabelas foram criadas (ou já existiam).")
