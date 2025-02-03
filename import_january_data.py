import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from unidecode import unidecode

# Configuração da conexão com o banco de dados
DATABASE_URL = "postgresql://postgres:fvPCqIuJkOHZxVtzPgmYbiYDbikhylXa@roundhouse.proxy.rlwy.net:10419/railway"

def connect_db():
    return psycopg2.connect(DATABASE_URL)

def generate_login(name):
    # Converte para minúsculo e remove acentos
    login = unidecode(name.lower())
    # Remove espaços e caracteres especiais
    login = ''.join(c for c in login if c.isalnum())
    # Limita a 20 caracteres
    return login[:20]

def convert_date(date_str):
    if pd.isna(date_str):
        return None
    try:
        # Se já for datetime, retorna como está
        if isinstance(date_str, pd.Timestamp):
            return date_str
            
        # Converte string para datetime
        return pd.to_datetime(date_str, format='%d/%m/%Y %H:%M')
    except:
        return None

def clean_contrato(contrato):
    if pd.isna(contrato):
        return 0
    try:
        # Remove qualquer caractere não numérico
        numero = ''.join(c for c in str(contrato) if c.isdigit())
        return int(numero) if numero else 0
    except:
        return 0

def import_data():
    print("🔄 Iniciando importação dos dados de Janeiro...")
    
    # Carregar o arquivo Excel
    df = pd.read_excel('dados_janeiro.xlsx')
    print(f"📊 Carregados {len(df)} registros do arquivo Excel")
    
    # Converter datas e contratos
    df['DATA'] = df['DATA'].apply(convert_date)
    df['CONTRATO'] = df['CONTRATO'].apply(clean_contrato)
    
    # Conectar ao banco
    conn = connect_db()
    cur = conn.cursor()
    
    try:
        # Desativa autocommit para melhor performance
        conn.autocommit = False
        
        # 1. Inserir Bases
        print("\n🏢 Importando bases...")
        bases = [(b,) for b in df['BASE'].dropna().unique()]
        execute_values(
            cur,
            "INSERT INTO bases (nome) VALUES %s ON CONFLICT (nome) DO NOTHING",
            bases
        )
        
        # 2. Inserir Técnicos
        print("\n👨‍🔧 Importando técnicos...")
        tecnicos = [(t, generate_login(t)) for t in df['TECNICO'].dropna().unique()]
        execute_values(
            cur,
            "INSERT INTO tecnicos (nome, login) VALUES %s ON CONFLICT (login) DO NOTHING",
            tecnicos
        )
        
        # 3. Inserir Status
        print("\n📊 Importando status...")
        status = [(s,) for s in df['STATUS'].dropna().unique()]
        execute_values(
            cur,
            "INSERT INTO status_os (nome) VALUES %s ON CONFLICT (nome) DO NOTHING",
            status
        )
        
        # 4. Inserir Tipos de Serviço
        print("\n🔧 Importando tipos de serviço...")
        tipos = [(t,) for t in df['TIPO DE SERVIÇO'].dropna().unique()]
        execute_values(
            cur,
            "INSERT INTO tipos_servico (nome) VALUES %s ON CONFLICT (nome) DO NOTHING",
            tipos
        )
        
        # 5. Criar tabela temporária para OS
        print("\n📝 Criando tabela temporária...")
        cur.execute("""
            CREATE TEMP TABLE temp_os (
                data_execucao TIMESTAMP,
                base_nome TEXT,
                tecnico_nome TEXT,
                status_nome TEXT,
                tipo_servico_nome TEXT,
                contrato INTEGER,
                latitude DECIMAL(10,8),
                longitude DECIMAL(11,8),
                valor_tecnico DECIMAL(10,2),
                valor_empresa DECIMAL(10,2)
            )
        """)
        
        # 6. Inserir dados na tabela temporária
        print("\n📥 Inserindo dados na tabela temporária...")
        dados_os = df.apply(lambda row: [
            row['DATA'],
            row['BASE'],
            row['TECNICO'],
            row['STATUS'],
            row['TIPO DE SERVIÇO'],
            row['CONTRATO'],
            float(row['LATIDUDE']) if pd.notna(row['LATIDUDE']) else 0,
            float(row['LONGITUDE']) if pd.notna(row['LONGITUDE']) else 0,
            float(str(row['VALOR TÉCNICO']).replace('R$', '').replace('.', '').replace(',', '.').strip()) if pd.notna(row['VALOR TÉCNICO']) else 0,
            float(str(row['VALOR EMPRESA']).replace('R$', '').replace('.', '').replace(',', '.').strip()) if pd.notna(row['VALOR EMPRESA']) else 0
        ], axis=1).tolist()
        
        execute_values(cur, """
            INSERT INTO temp_os VALUES %s
        """, dados_os)
        
        # 7. Inserir OS do temp para a tabela final
        print("\n📤 Inserindo ordens de serviço...")
        cur.execute("""
            INSERT INTO ordens_servico (
                data_execucao, contrato, latitude, longitude,
                valor_tecnico, valor_empresa,
                base_id, tecnico_id, status_id, tipo_servico_id
            )
            SELECT 
                t.data_execucao, t.contrato, t.latitude, t.longitude,
                t.valor_tecnico, t.valor_empresa,
                b.id, tc.id, s.id, ts.id
            FROM temp_os t
            LEFT JOIN bases b ON b.nome = t.base_nome
            LEFT JOIN tecnicos tc ON tc.nome = t.tecnico_nome
            LEFT JOIN status_os s ON s.nome = t.status_nome
            LEFT JOIN tipos_servico ts ON ts.nome = t.tipo_servico_nome
        """)
        
        # Commit das alterações
        conn.commit()
        print("\n✅ Importação concluída com sucesso!")
        
        # 8. Mostrar estatísticas
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
