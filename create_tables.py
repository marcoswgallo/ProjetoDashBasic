import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import Error

def create_tables():
    load_dotenv()
    
    # SQL para criar as tabelas
    commands = [
        """
        CREATE TABLE IF NOT EXISTS supervisores (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100) UNIQUE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tecnicos (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100) NOT NULL,
            login VARCHAR(50) UNIQUE NOT NULL,
            supervisor_id INTEGER REFERENCES supervisores(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS bases (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100) UNIQUE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tipos_servico (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100) UNIQUE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS status_os (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(50) UNIQUE NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ordens_servico (
            id SERIAL PRIMARY KEY,
            data_toa TIMESTAMP,
            data_execucao TIMESTAMP,
            base_id INTEGER REFERENCES bases(id),
            tipo_servico_id INTEGER REFERENCES tipos_servico(id),
            status_id INTEGER REFERENCES status_os(id),
            tecnico_id INTEGER REFERENCES tecnicos(id),
            contrato BIGINT,
            wo VARCHAR(50),
            os VARCHAR(50),
            cliente VARCHAR(200),
            endereco VARCHAR(500),
            bairro VARCHAR(100),
            cidade VARCHAR(100),
            latitude DECIMAL(10, 8),
            longitude DECIMAL(11, 8),
            node VARCHAR(100),
            local_tipo VARCHAR(20),
            inicio TIMESTAMP,
            fim TIMESTAMP,
            valor_tecnico DECIMAL(10, 2),
            valor_empresa DECIMAL(10, 2),
            cod_status VARCHAR(20),
            tipo_residencia VARCHAR(20),
            pacote VARCHAR(200),
            pdf_status VARCHAR(5),
            foto_status VARCHAR(5),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        # Índices para melhorar performance
        """
        CREATE INDEX IF NOT EXISTS idx_os_data_execucao ON ordens_servico(data_execucao);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_os_contrato ON ordens_servico(contrato);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_os_status ON ordens_servico(status_id);
        """
    ]
    
    conn = None
    try:
        # Conecta ao banco de dados
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        
        # Cria cada tabela
        for command in commands:
            cur.execute(command)
            
        # Commit das alterações
        conn.commit()
        
        print("✅ Tabelas criadas com sucesso!")
        
    except (Exception, Error) as error:
        print(f"❌ Erro ao criar as tabelas: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == '__main__':
    create_tables()
