import pandas as pd
import psycopg2
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Carregando variáveis de ambiente
load_dotenv()

# SQL connection
sql_conn = psycopg2.connect(os.getenv("SQL_URL"))

# Read data from SQL
query = """
    SELECT id, nome, sigla, status
      FROM empresa;
"""
df = pd.read_sql_query(query, sql_conn)
sql_conn.close()

# MongoDB connection
mongo_client = MongoClient(os.getenv("MONGO_URL"))

mongo_db = mongo_client["historico_chat"]
mongo_empresa = mongo_db["empresas"]

# Percorrendo dataframe
for _, row in df.iterrows():
    record = {
        "_id": row.get("id"),
        "nome": row.get("nome"),
        "sigla": row.get("sigla"),
        "status": row.get("status")
    }

    # Fazendo upsert das empresas no mongo
    mongo_empresa.update_one(
        {"_id": record.get("_id")}, # Condição do match
        {"$set": record}, # Atualizando campos
        upsert=True
    )

mongo_client.close()
