import os
from dotenv import load_dotenv
import psycopg2

def clean_database():
    load_dotenv()
    
    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Lista de tabelas para limpar
        tables = [
            'ordens_servico',
            'tecnicos',
            'supervisores',
            'bases',
            'tipos_servico',
            'status_os'
        ]
        
        # Desativa verificações de chave estrangeira temporariamente
        cur.execute("SET session_replication_role = 'replica';")
        
        # Limpa cada tabela
        for table in tables:
            print(f"Limpando tabela {table}...")
            cur.execute(f"TRUNCATE TABLE {table} CASCADE;")
        
        # Reativa verificações de chave estrangeira
        cur.execute("SET session_replication_role = 'origin';")
        
        conn.commit()
        print("✅ Banco de dados limpo com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao limpar banco de dados: {str(e)}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    clean_database()
