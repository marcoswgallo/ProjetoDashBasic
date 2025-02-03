import os
from dotenv import load_dotenv
import psycopg2

def verify_data():
    load_dotenv()
    
    try:
        # Conexão com o banco de dados
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Total de ordens de serviço
        cur.execute("SELECT COUNT(*) FROM ordens_servico")
        total_os = cur.fetchone()[0]
        print(f"\nTotal de Ordens de Serviço: {total_os}")
        
        # Total de contratos únicos
        cur.execute("SELECT COUNT(DISTINCT contrato) FROM ordens_servico")
        total_contratos = cur.fetchone()[0]
        print(f"Total de Contratos Únicos: {total_contratos}")
        
        # Total de técnicos
        cur.execute("SELECT COUNT(*) FROM tecnicos")
        total_tecnicos = cur.fetchone()[0]
        print(f"Total de Técnicos: {total_tecnicos}")
        
        # Total de supervisores
        cur.execute("SELECT COUNT(*) FROM supervisores")
        total_supervisores = cur.fetchone()[0]
        print(f"Total de Supervisores: {total_supervisores}")
        
        # Amostra de dados
        print("\nAmostra de Ordens de Serviço:")
        cur.execute("""
            SELECT os.contrato, os.data_execucao, b.nome as base, t.nome as tecnico, s.nome as status
            FROM ordens_servico os
            LEFT JOIN bases b ON b.id = os.base_id
            LEFT JOIN tecnicos t ON t.id = os.tecnico_id
            LEFT JOIN status_os s ON s.id = os.status_id
            LIMIT 5
        """)
        for row in cur.fetchall():
            print(f"Contrato: {row[0]}, Data: {row[1]}, Base: {row[2]}, Técnico: {row[3]}, Status: {row[4]}")
            
    except Exception as e:
        print(f"Erro ao verificar dados: {str(e)}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    verify_data()
