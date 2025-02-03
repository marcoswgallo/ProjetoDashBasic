import pandas as pd
import psycopg2

def test_query():
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect("postgresql://postgres:fvPCqIuJkOHZxVtzPgmYbiYDbikhylXa@roundhouse.proxy.rlwy.net:10419/railway")
        
        # Query completa sem limite
        query = """
        SELECT 
            os.id, os.data_execucao, os.contrato, os.latitude, os.longitude,
            os.valor_tecnico, os.valor_empresa,
            b.nome as base, t.nome as tecnico, s.nome as status,
            ts.nome as tipo_servico
        FROM ordens_servico os
        LEFT JOIN bases b ON b.id = os.base_id
        LEFT JOIN tecnicos t ON t.id = os.tecnico_id
        LEFT JOIN status_os s ON s.id = os.status_id
        LEFT JOIN tipos_servico ts ON ts.id = os.tipo_servico_id
        WHERE os.data_execucao IS NOT NULL
        """
        
        print("Executando query...")
        df = pd.read_sql(query, conn)
        print(f"Total de registros retornados: {len(df):,}")
        
        # Mostrar algumas estatísticas
        print("\nEstatísticas:")
        print(f"Memória utilizada: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        print(f"Colunas: {', '.join(df.columns)}")
        print(f"\nPrimeiros registros:")
        print(df.head())
        
    except Exception as e:
        print(f"Erro: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    test_query()
