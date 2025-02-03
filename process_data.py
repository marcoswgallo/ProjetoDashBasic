import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Error
from datetime import datetime
import re

def clean_currency(value):
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return value
    # Remove R$, espaços e converte vírgula para ponto
    value = str(value).replace('R$', '').replace(' ', '').replace('.', '').replace(',', '.')
    try:
        return float(value)
    except:
        return None

def clean_date(value):
    if pd.isna(value):
        return None
    try:
        if isinstance(value, str):
            # Tenta diferentes formatos de data
            for fmt in ['%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%d/%m/%Y']:
                try:
                    return pd.to_datetime(value, format=fmt)
                except:
                    continue
        return pd.to_datetime(value)
    except:
        return None

def process_data():
    load_dotenv()
    
    try:
        print("📊 Lendo arquivo Excel...")
        df = pd.read_excel('dados.xlsx')
        print(f"Total de registros: {len(df)}")
        
        # Conexão com o banco de dados
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Limpeza e transformação dos dados
        print("🧹 Limpando e transformando dados...")
        
        # Tratamento de datas
        print("📅 Processando datas...")
        df['DATA_TOA'] = df['DATA_TOA'].apply(clean_date)
        df['DATA'] = df['DATA'].apply(clean_date)
        df['INÍCIO'] = df['INÍCIO'].apply(clean_date)
        df['FIM'] = df['FIM'].apply(clean_date)
        
        # Tratamento de valores monetários
        print("💰 Processando valores monetários...")
        df['VALOR_TÉCNICO'] = df['VALOR TÉCNICO'].apply(clean_currency)
        df['VALOR_EMPRESA'] = df['VALOR EMPRESA'].apply(clean_currency)
        
        # Inserção de Supervisores
        print("👥 Inserindo supervisores...")
        supervisores = df['SUPERVISOR'].dropna().unique()
        for supervisor in supervisores:
            try:
                cur.execute(
                    "INSERT INTO supervisores (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING",
                    (supervisor,)
                )
                conn.commit()
            except Exception as e:
                print(f"Erro ao inserir supervisor {supervisor}: {str(e)}")
                conn.rollback()
        
        # Inserção de Técnicos
        print("👨‍🔧 Inserindo técnicos...")
        tecnicos_df = df[['TECNICO', 'LOGIN', 'SUPERVISOR']].drop_duplicates()
        for _, tecnico in tecnicos_df.iterrows():
            try:
                if pd.notna(tecnico['SUPERVISOR']):
                    cur.execute(
                        """
                        WITH sup AS (SELECT id FROM supervisores WHERE nome = %s)
                        INSERT INTO tecnicos (nome, login, supervisor_id)
                        VALUES (%s, %s, (SELECT id FROM sup))
                        ON CONFLICT (login) DO NOTHING
                        """,
                        (tecnico['SUPERVISOR'], tecnico['TECNICO'], tecnico['LOGIN'])
                    )
                    conn.commit()
            except Exception as e:
                print(f"Erro ao inserir técnico {tecnico['TECNICO']}: {str(e)}")
                conn.rollback()
        
        # Inserção de Bases
        print("🏢 Inserindo bases...")
        bases = df['BASE'].dropna().unique()
        for base in bases:
            try:
                cur.execute(
                    "INSERT INTO bases (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING",
                    (base,)
                )
                conn.commit()
            except Exception as e:
                print(f"Erro ao inserir base {base}: {str(e)}")
                conn.rollback()
        
        # Inserção de Tipos de Serviço
        print("🔧 Inserindo tipos de serviço...")
        tipos_servico = df['SERVIÇO'].dropna().unique()
        for tipo in tipos_servico:
            try:
                cur.execute(
                    "INSERT INTO tipos_servico (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING",
                    (tipo,)
                )
                conn.commit()
            except Exception as e:
                print(f"Erro ao inserir tipo de serviço {tipo}: {str(e)}")
                conn.rollback()
        
        # Inserção de Status
        print("📋 Inserindo status...")
        status_list = df['STATUS'].dropna().unique()
        for status in status_list:
            try:
                cur.execute(
                    "INSERT INTO status_os (nome) VALUES (%s) ON CONFLICT (nome) DO NOTHING",
                    (status,)
                )
                conn.commit()
            except Exception as e:
                print(f"Erro ao inserir status {status}: {str(e)}")
                conn.rollback()
        
        # Inserção das Ordens de Serviço
        print("📝 Inserindo ordens de serviço...")
        batch_size = 100
        total_rows = len(df)
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            print(f"Processando registros {i+1} até {min(i+batch_size, total_rows)} de {total_rows}")
            
            for _, row in batch_df.iterrows():
                try:
                    cur.execute(
                        """
                        WITH base_id AS (SELECT id FROM bases WHERE nome = %s),
                             tipo_id AS (SELECT id FROM tipos_servico WHERE nome = %s),
                             status_id AS (SELECT id FROM status_os WHERE nome = %s),
                             tec_id AS (SELECT id FROM tecnicos WHERE login = %s)
                        INSERT INTO ordens_servico (
                            data_toa, data_execucao, base_id, tipo_servico_id, status_id,
                            tecnico_id, contrato, wo, os, cliente, endereco, bairro,
                            cidade, latitude, longitude, node, local_tipo, inicio, fim,
                            valor_tecnico, valor_empresa, cod_status, tipo_residencia,
                            pacote, pdf_status, foto_status
                        )
                        VALUES (
                            %s, %s, (SELECT id FROM base_id), (SELECT id FROM tipo_id),
                            (SELECT id FROM status_id), (SELECT id FROM tec_id),
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s
                        )
                        """,
                        (
                            row['BASE'], row['SERVIÇO'], row['STATUS'], row['LOGIN'],
                            row['DATA_TOA'], row['DATA'],
                            row['CONTRATO'], row['WO'], row['OS'], row['CLIENTE'],
                            row['ENDEREÇO'], row['BAIRRO'], row['CIDADES'],
                            row['LATIDUDE'], row['LONGITUDE'], row['NODE'],
                            row['LOCAL'], row['INÍCIO'], row['FIM'],
                            row['VALOR_TÉCNICO'], row['VALOR_EMPRESA'],
                            row['COD STATUS'], row['TIPO RESIDÊNCIA'],
                            row['PACOTE'], row['PDF'], row['FOTO']
                        )
                    )
                    conn.commit()
                except Exception as e:
                    print(f"Erro ao inserir OS {row['OS']}: {str(e)}")
                    conn.rollback()
        
        print("✅ Dados processados e inseridos com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro durante o processamento: {str(e)}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    process_data()
