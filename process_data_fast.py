import pandas as pd
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

def convert_date(date_str):
    if pd.isna(date_str) or str(date_str).lower() in ['nan', 'nat']:
        return None
    try:
        # Se já for datetime, converte para string ISO
        if isinstance(date_str, pd.Timestamp):
            return date_str.strftime('%Y-%m-%d %H:%M:%S')
            
        date_str = str(date_str).strip()
        
        # Tenta diferentes formatos de data
        formats = [
            '%d/%m/%Y %H:%M',
            '%d/%m/%Y %H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y',
            '%Y-%m-%d'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
                
        return None
    except:
        return None

def process_data_fast():
    load_dotenv()
    
    try:
        print(" Lendo arquivo Excel...")
        df = pd.read_excel('dados.xlsx')
        print(f"Total de registros: {len(df)}")
        
        # Conexão com o banco de dados
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Desativa autocommit para melhor performance
        conn.autocommit = False
        
        # 1. Inserir Supervisores
        print(" Inserindo supervisores...")
        supervisores = [(s,) for s in df['SUPERVISOR'].dropna().unique()]
        execute_values(
            cur,
            "INSERT INTO supervisores (nome) VALUES %s ON CONFLICT (nome) DO NOTHING",
            supervisores
        )
        
        # 2. Inserir Bases
        print(" Inserindo bases...")
        bases = [(b,) for b in df['BASE'].dropna().unique()]
        execute_values(
            cur,
            "INSERT INTO bases (nome) VALUES %s ON CONFLICT (nome) DO NOTHING",
            bases
        )
        
        # 3. Inserir Tipos de Serviço
        print(" Inserindo tipos de serviço...")
        servicos = [(s,) for s in df['SERVIÇO'].dropna().unique()]
        execute_values(
            cur,
            "INSERT INTO tipos_servico (nome) VALUES %s ON CONFLICT (nome) DO NOTHING",
            servicos
        )
        
        # 4. Inserir Status
        print(" Inserindo status...")
        status = [(s,) for s in df['STATUS'].dropna().unique()]
        execute_values(
            cur,
            "INSERT INTO status_os (nome) VALUES %s ON CONFLICT (nome) DO NOTHING",
            status
        )
        
        # 5. Inserir Técnicos
        print(" Inserindo técnicos...")
        tecnicos_df = df[['TECNICO', 'LOGIN', 'SUPERVISOR']].drop_duplicates().dropna()
        for _, row in tecnicos_df.iterrows():
            cur.execute("""
                WITH sup AS (SELECT id FROM supervisores WHERE nome = %s)
                INSERT INTO tecnicos (nome, login, supervisor_id)
                VALUES (%s, %s, (SELECT id FROM sup))
                ON CONFLICT (login) DO NOTHING
            """, (row['SUPERVISOR'], row['TECNICO'], row['LOGIN']))
        
        # 6. Preparar e inserir Ordens de Serviço
        print(" Preparando dados das ordens de serviço...")
        
        # Criar tabela temporária para importação em massa
        cur.execute("""
            CREATE TEMP TABLE temp_os (
                data_toa TEXT, data_execucao TEXT, base_nome TEXT, 
                tipo_servico_nome TEXT, status_nome TEXT, tecnico_login TEXT,
                contrato BIGINT, wo TEXT, os TEXT, cliente TEXT, 
                endereco TEXT, bairro TEXT, cidade TEXT,
                latitude DECIMAL(10,8), longitude DECIMAL(11,8), 
                node TEXT, local_tipo TEXT, inicio TEXT, fim TEXT,
                valor_tecnico TEXT, valor_empresa TEXT, 
                cod_status TEXT, tipo_residencia TEXT,
                pacote TEXT, pdf_status TEXT, foto_status TEXT
            )
        """)
        
        print(" Inserindo dados na tabela temporária...")
        # Preparar dados para inserção
        dados_os = df.apply(lambda row: [
            convert_date(row['DATA_TOA']),
            convert_date(row['DATA']),
            row['BASE'],
            row['SERVIÇO'],
            row['STATUS'],
            row['LOGIN'],
            row['CONTRATO'],
            row['WO'],
            row['OS'],
            row['CLIENTE'],
            row['ENDEREÇO'],
            row['BAIRRO'],
            row['CIDADES'],
            row['LATIDUDE'],
            row['LONGITUDE'],
            row['NODE'],
            row['LOCAL'],
            convert_date(row['INÍCIO']),
            convert_date(row['FIM']),
            str(row['VALOR TÉCNICO']),
            str(row['VALOR EMPRESA']),
            row['COD STATUS'],
            row['TIPO RESIDÊNCIA'],
            row['PACOTE'],
            row['PDF'],
            row['FOTO']
        ], axis=1).tolist()
        
        execute_values(cur, """
            INSERT INTO temp_os VALUES %s
        """, dados_os)
        
        print(" Transferindo dados para a tabela final...")
        # Inserir da tabela temporária para a tabela final
        cur.execute("""
            INSERT INTO ordens_servico (
                data_toa, data_execucao, base_id, tipo_servico_id, 
                status_id, tecnico_id, contrato, wo, os, cliente,
                endereco, bairro, cidade, latitude, longitude,
                node, local_tipo, inicio, fim, valor_tecnico,
                valor_empresa, cod_status, tipo_residencia,
                pacote, pdf_status, foto_status
            )
            SELECT 
                CASE 
                    WHEN t.data_toa IS NULL OR t.data_toa = 'None' THEN NULL 
                    ELSE t.data_toa::timestamp
                END,
                CASE 
                    WHEN t.data_execucao IS NULL OR t.data_execucao = 'None' THEN NULL 
                    ELSE t.data_execucao::timestamp
                END,
                b.id, ts.id, s.id, tec.id,
                t.contrato, t.wo, t.os, 
                NULLIF(t.cliente, 'nan'),
                NULLIF(t.endereco, 'nan'),
                NULLIF(t.bairro, 'nan'),
                NULLIF(t.cidade, 'nan'),
                CASE 
                    WHEN t.latitude::text = 'nan' THEN NULL 
                    ELSE t.latitude::decimal
                END,
                CASE 
                    WHEN t.longitude::text = 'nan' THEN NULL 
                    ELSE t.longitude::decimal
                END,
                NULLIF(t.node, 'nan'),
                NULLIF(t.local_tipo, 'nan'),
                CASE 
                    WHEN t.inicio IS NULL OR t.inicio = 'None' THEN NULL 
                    ELSE t.inicio::timestamp
                END,
                CASE 
                    WHEN t.fim IS NULL OR t.fim = 'None' THEN NULL 
                    ELSE t.fim::timestamp
                END,
                CASE 
                    WHEN t.valor_tecnico = 'nan' THEN NULL 
                    ELSE REPLACE(REPLACE(t.valor_tecnico, 'R$', ''), ',', '.')::decimal
                END,
                CASE 
                    WHEN t.valor_empresa = 'nan' THEN NULL 
                    ELSE REPLACE(REPLACE(t.valor_empresa, 'R$', ''), ',', '.')::decimal
                END,
                NULLIF(t.cod_status, 'nan'),
                NULLIF(t.tipo_residencia, 'nan'),
                NULLIF(t.pacote, 'nan'),
                NULLIF(t.pdf_status, 'nan'),
                NULLIF(t.foto_status, 'nan')
            FROM temp_os t
            LEFT JOIN bases b ON b.nome = t.base_nome
            LEFT JOIN tipos_servico ts ON ts.nome = t.tipo_servico_nome
            LEFT JOIN status_os s ON s.nome = t.status_nome
            LEFT JOIN tecnicos tec ON tec.login = t.tecnico_login
        """)
        
        # Commit todas as alterações
        conn.commit()
        print(" Dados processados e inseridos com sucesso!")
        
        # Contar registros inseridos
        cur.execute("SELECT COUNT(*) FROM ordens_servico")
        total_registros = cur.fetchone()[0]
        print(f" Total de registros inseridos: {total_registros}")
        
    except Exception as e:
        conn.rollback()
        print(f" Erro durante o processamento: {str(e)}")
        raise e
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    process_data_fast()
