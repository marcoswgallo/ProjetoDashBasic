import psycopg2

def check_db_config():
    try:
        # Conectar ao banco de dados
        conn = psycopg2.connect("postgresql://postgres:fvPCqIuJkOHZxVtzPgmYbiYDbikhylXa@roundhouse.proxy.rlwy.net:10419/railway")
        cur = conn.cursor()

        # Verificar configurações importantes
        configs = [
            'max_connections',
            'statement_timeout',
            'idle_in_transaction_session_timeout',
            'row_security',
            'max_locks_per_transaction'
        ]

        print("Configurações do PostgreSQL:")
        print("-" * 50)
        
        for config in configs:
            cur.execute(f"SHOW {config};")
            value = cur.fetchone()[0]
            print(f"{config}: {value}")

        # Verificar limites de recursos
        cur.execute("""
            SELECT COUNT(*) 
            FROM ordens_servico;
        """)
        total_rows = cur.fetchone()[0]
        print(f"\nTotal de registros na tabela ordens_servico: {total_rows:,}")

        # Verificar se há algum limite na consulta
        print("\nTestando consulta sem limite...")
        cur.execute("""
            SELECT COUNT(*) 
            FROM (
                SELECT os.id
                FROM ordens_servico os
                LEFT JOIN bases b ON b.id = os.base_id
                LEFT JOIN tecnicos t ON t.id = os.tecnico_id
                LEFT JOIN status_os s ON s.id = os.status_id
                LEFT JOIN tipos_servico ts ON ts.id = os.tipo_servico_id
            ) subq;
        """)
        total_joined = cur.fetchone()[0]
        print(f"Total de registros com JOINs: {total_joined:,}")

    except Exception as e:
        print(f"Erro: {str(e)}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    check_db_config()
