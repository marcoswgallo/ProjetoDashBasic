import psycopg2

# Configuração da conexão com o banco de dados
DATABASE_URL = "postgresql://postgres:fvPCqIuJkOHZxVtzPgmYbiYDbikhylXa@roundhouse.proxy.rlwy.net:10419/railway"

def add_constraints():
    print("🔄 Adicionando constraints de unicidade...")
    
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    try:
        # Adicionar constraints de unicidade
        cur.execute("""
            ALTER TABLE bases 
            ADD CONSTRAINT bases_nome_key UNIQUE (nome);
        """)
        
        cur.execute("""
            ALTER TABLE tecnicos 
            ADD CONSTRAINT tecnicos_nome_key UNIQUE (nome);
        """)
        
        cur.execute("""
            ALTER TABLE status_os 
            ADD CONSTRAINT status_os_nome_key UNIQUE (nome);
        """)
        
        cur.execute("""
            ALTER TABLE tipos_servico 
            ADD CONSTRAINT tipos_servico_nome_key UNIQUE (nome);
        """)
        
        cur.execute("""
            ALTER TABLE ordens_servico 
            ADD CONSTRAINT ordens_servico_contrato_key UNIQUE (contrato);
        """)
        
        conn.commit()
        print("✅ Constraints adicionadas com sucesso!")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao adicionar constraints: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    add_constraints()
