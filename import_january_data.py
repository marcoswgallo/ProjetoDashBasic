import pandas as pd
import psycopg2
from datetime import datetime
import numpy as np

# Configuração da conexão com o banco de dados
DATABASE_URL = "postgresql://postgres:fvPCqIuJkOHZxVtzPgmYbiYDbikhylXa@roundhouse.proxy.rlwy.net:10419/railway"

def connect_db():
    return psycopg2.connect(DATABASE_URL)

def import_data():
    print("🔄 Iniciando importação dos dados de Janeiro...")
    
    # Carregar o arquivo Excel
    df = pd.read_excel('dados_janeiro.xlsx')
    print(f"📊 Carregados {len(df)} registros do arquivo Excel")
    
    # Conectar ao banco
    conn = connect_db()
    cur = conn.cursor()
    
    try:
        # 1. Importar Bases
        print("\n🏢 Importando bases...")
        bases = df['BASE'].unique()
        for base in bases:
            cur.execute("""
                INSERT INTO bases (nome)
                VALUES (%s)
                ON CONFLICT (nome) DO NOTHING
                RETURNING id
            """, (base,))
        conn.commit()
        
        # 2. Importar Técnicos
        print("\n👨‍🔧 Importando técnicos...")
        tecnicos = df['TECNICO'].unique()
        for tecnico in tecnicos:
            cur.execute("""
                INSERT INTO tecnicos (nome)
                VALUES (%s)
                ON CONFLICT (nome) DO NOTHING
                RETURNING id
            """, (tecnico,))
        conn.commit()
        
        # 3. Importar Status
        print("\n📊 Importando status...")
        status_list = df['STATUS'].unique()
        for status in status_list:
            cur.execute("""
                INSERT INTO status_os (nome)
                VALUES (%s)
                ON CONFLICT (nome) DO NOTHING
                RETURNING id
            """, (status,))
        conn.commit()
        
        # 4. Importar Tipos de Serviço
        print("\n🔧 Importando tipos de serviço...")
        tipos = df['TIPO'].unique()
        for tipo in tipos:
            cur.execute("""
                INSERT INTO tipos_servico (nome)
                VALUES (%s)
                ON CONFLICT (nome) DO NOTHING
                RETURNING id
            """, (tipo,))
        conn.commit()
        
        # 5. Importar Ordens de Serviço
        print("\n📝 Importando ordens de serviço...")
        for _, row in df.iterrows():
            # Obter IDs
            cur.execute("SELECT id FROM bases WHERE nome = %s", (row['BASE'],))
            base_id = cur.fetchone()[0]
            
            cur.execute("SELECT id FROM tecnicos WHERE nome = %s", (row['TECNICO'],))
            tecnico_id = cur.fetchone()[0]
            
            cur.execute("SELECT id FROM status_os WHERE nome = %s", (row['STATUS'],))
            status_id = cur.fetchone()[0]
            
            cur.execute("SELECT id FROM tipos_servico WHERE nome = %s", (row['TIPO'],))
            tipo_servico_id = cur.fetchone()[0]
            
            # Inserir OS
            cur.execute("""
                INSERT INTO ordens_servico (
                    contrato, data_execucao, latitude, longitude,
                    valor_tecnico, valor_empresa,
                    base_id, tecnico_id, status_id, tipo_servico_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (contrato) DO UPDATE SET
                    data_execucao = EXCLUDED.data_execucao,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    valor_tecnico = EXCLUDED.valor_tecnico,
                    valor_empresa = EXCLUDED.valor_empresa,
                    base_id = EXCLUDED.base_id,
                    tecnico_id = EXCLUDED.tecnico_id,
                    status_id = EXCLUDED.status_id,
                    tipo_servico_id = EXCLUDED.tipo_servico_id
            """, (
                row['CONTRATO'],
                row['DATA'],
                row['LATITUDE'],
                row['LONGITUDE'],
                float(str(row['VALOR TECNICO']).replace('R$', '').replace('.', '').replace(',', '.').strip()),
                float(str(row['VALOR EMPRESA']).replace('R$', '').replace('.', '').replace(',', '.').strip()),
                base_id,
                tecnico_id,
                status_id,
                tipo_servico_id
            ))
            
        conn.commit()
        print("\n✅ Importação concluída com sucesso!")
        
        # 6. Mostrar estatísticas
        cur.execute("SELECT COUNT(*) FROM ordens_servico")
        total_os = cur.fetchone()[0]
        print(f"\n📊 Estatísticas:")
        print(f"- Total de Ordens de Serviço: {total_os}")
        print(f"- Bases cadastradas: {len(bases)}")
        print(f"- Técnicos cadastrados: {len(tecnicos)}")
        print(f"- Tipos de serviço: {len(tipos)}")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Erro durante a importação: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    import_data()
