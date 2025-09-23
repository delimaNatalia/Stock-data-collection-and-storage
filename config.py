from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os


load_dotenv()


DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")  
DB_PORT = os.getenv("DB_PORT", "5432")       


symbol = 'winfut'
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{symbol}')
R_values =[8, 13, 40]
tick = 5

INPUT_FOLDER_PATH = 'input_data' 
OUTPUT_FOLDER_PATH = 'output_data' 


#Para testar a conexão
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))  
        print("Conexão OK:", result.fetchone())
except Exception as e:
    print("Erro ao conectar:", e)



