import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError

def test_connection():
    load_dotenv()  # Carrega as variáveis do arquivo .env
    
    try:
        # Obtém a URL do banco de dados do arquivo .env
        database_url = os.getenv('DATABASE_URL')
        
        # Tenta estabelecer a conexão
        connection = psycopg2.connect(database_url)
        
        # Se chegou aqui, a conexão foi bem sucedida
        print("✅ Conexão com o banco de dados estabelecida com sucesso!")
        
        # Obtém a versão do PostgreSQL
        cursor = connection.cursor()
        cursor.execute('SELECT version();')
        db_version = cursor.fetchone()
        print(f"Versão do PostgreSQL: {db_version[0]}")
        
        cursor.close()
        connection.close()
        print("Conexão fechada.")
        
    except OperationalError as e:
        print("❌ Erro ao conectar ao banco de dados:")
        print(e)
    except Exception as e:
        print("❌ Ocorreu um erro inesperado:")
        print(e)

if __name__ == "__main__":
    test_connection()
