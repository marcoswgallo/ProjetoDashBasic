import psycopg2

# Configuração da conexão com o banco de dados
DATABASE_URL = "postgresql://postgres:fvPCqIuJkOHZxVtzPgmYbiYDbikhylXa@roundhouse.proxy.rlwy.net:10419/railway"

def add_constraint():
    print("🔄 Adicionando constraint de unicidade no contrato...")
    
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    try:
        # Adicionar constraint de unicidade
        cur.execute("""
            ALTER TABLE ordens_servico 
            ADD CONSTRAINT ordens_servico_contrato_key UNIQUE (contrato);
        """)
        
        conn.commit()
        print("✅ Constraint adicionada com sucesso!")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Erro ao adicionar constraint: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    add_constraint()
