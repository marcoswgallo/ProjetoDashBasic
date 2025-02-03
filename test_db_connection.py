import psycopg2
import os
from dotenv import load_dotenv

def test_connection():
    try:
        # Tenta conectar ao banco
        conn = psycopg2.connect("postgresql://postgres:fvPCqIuJkOHZxVtzPgmYbiYDbikhylXa@roundhouse.proxy.rlwy.net:10419/railway")
        
        # Cria um cursor
        cur = conn.cursor()
        
        # Lista todas as tabelas do banco
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        
        tables = cur.fetchall()
        print("Conexão bem sucedida!")
        print("\nTabelas encontradas:")
        for table in tables:
            print(f"- {table[0]}")
            
            # Conta o número de registros em cada tabela
            cur.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cur.fetchone()[0]
            print(f"  Registros: {count}")
        
        # Fecha cursor e conexão
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Erro ao conectar: {str(e)}")

if __name__ == "__main__":
    test_connection()
