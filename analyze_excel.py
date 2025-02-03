import pandas as pd
import numpy as np
from datetime import datetime

def analyze_excel():
    try:
        # Lê o arquivo Excel
        print("📊 Analisando o arquivo dados.xlsx...")
        df = pd.read_excel('dados.xlsx')
        
        # Informações básicas
        print("\n📋 Informações Básicas:")
        print(f"Total de linhas: {len(df)}")
        print(f"Total de colunas: {len(df.columns)}")
        print("\n📑 Colunas encontradas:")
        for col in df.columns:
            print(f"- {col}")
        
        # Análise de tipos de dados
        print("\n🔍 Tipos de dados por coluna:")
        print(df.dtypes)
        
        # Verificar valores nulos
        print("\n🔎 Valores nulos por coluna:")
        nulls = df.isnull().sum()
        for col, null_count in nulls.items():
            if null_count > 0:
                print(f"- {col}: {null_count} valores nulos")
        
        # Análise de datas
        date_columns = df.select_dtypes(include=['datetime64']).columns
        print("\n📅 Análise de datas:")
        for col in date_columns:
            print(f"\nColuna: {col}")
            print(f"Data mais antiga: {df[col].min()}")
            print(f"Data mais recente: {df[col].max()}")
        
        # Análise de valores numéricos
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        print("\n💰 Análise de valores numéricos:")
        for col in numeric_columns:
            print(f"\nColuna: {col}")
            print(f"Mínimo: {df[col].min()}")
            print(f"Máximo: {df[col].max()}")
            print(f"Média: {df[col].mean():.2f}")
        
        # Valores únicos em colunas categóricas
        categorical_columns = df.select_dtypes(include=['object']).columns
        print("\n📊 Análise de valores categóricos:")
        for col in categorical_columns:
            unique_values = df[col].nunique()
            print(f"\nColuna: {col}")
            print(f"Valores únicos: {unique_values}")
            if unique_values < 10:  # Mostra apenas se houver poucos valores únicos
                print("Valores:")
                print(df[col].value_counts().head())
        
        return df
        
    except FileNotFoundError:
        print("❌ Erro: O arquivo dados.xlsx não foi encontrado no diretório atual.")
    except Exception as e:
        print(f"❌ Erro ao analisar o arquivo: {str(e)}")

if __name__ == "__main__":
    df = analyze_excel()
