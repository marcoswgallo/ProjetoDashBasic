import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import os
from dotenv import load_dotenv
import psycopg2
from datetime import datetime, timedelta
import folium
from streamlit_folium import folium_static

# Configuração da página
st.set_page_config(page_title="Dashboard de Serviços", layout="wide")

# Função para conectar ao banco de dados
def get_connection():
    try:
        st.write(" Conectando ao banco de dados...")
        
        # URL pública do PostgreSQL (funciona tanto em dev quanto em prod)
        database_url = "postgresql://postgres:fvPCqIuJkOHZxVtzPgmYbiYDbikhylXa@roundhouse.proxy.rlwy.net:10419/railway"
        
        st.info(f" Tentando conectar ao banco de dados...")
        conn = psycopg2.connect(database_url)
        st.success(" Conectado ao banco de dados com sucesso!")
        return conn
    except psycopg2.Error as e:
        st.error(f" Erro ao conectar ao banco de dados: {str(e)}")
        st.info("Detalhes técnicos para debug:")
        st.code(f"""
        Erro: {type(e).__name__}
        Mensagem: {str(e)}
        pgcode: {getattr(e, 'pgcode', 'N/A')}
        pgerror: {getattr(e, 'pgerror', 'N/A')}
        diag: {getattr(e, 'diag', 'N/A')}
        """)
        st.stop()
    except Exception as e:
        st.error(f" Erro inesperado: {str(e)}")
        st.info("Por favor, recarregue a página. Se o erro persistir, entre em contato com o suporte.")
        st.stop()

# Função para carregar dados
@st.cache_data(ttl=3600)
def load_data():
    try:
        st.write(" Carregando dados...")
        conn = get_connection()
        
        # Query principal
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
        
        df = pd.read_sql(query, conn)
        conn.close()
        st.success(f" Dados carregados com sucesso! ({len(df)} registros)")
        return df
    except Exception as e:
        st.error(f" Erro ao carregar dados: {str(e)}")
        st.info("Detalhes técnicos para debug:")
        st.code(str(e))
        st.stop()

try:
    # Carrega os dados
    df = load_data()

    # Sidebar
    st.sidebar.title("Filtros")

    # Filtro de data
    min_date = df['data_execucao'].min()
    max_date = df['data_execucao'].max()
    date_range = st.sidebar.date_input(
        "Período",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # Filtro de base
    bases = ['Todas'] + sorted(df['base'].unique().tolist())
    selected_base = st.sidebar.selectbox('Base', bases)

    # Aplicar filtros
    mask = (df['data_execucao'].dt.date >= date_range[0]) & (df['data_execucao'].dt.date <= date_range[1])
    if selected_base != 'Todas':
        mask &= (df['base'] == selected_base)
    filtered_df = df[mask]

    # Layout principal
    st.title("Dashboard de Análise de Serviços")

    # Métricas principais
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total de Serviços", len(filtered_df))
    with col2:
        st.metric("Total de Técnicos", filtered_df['tecnico'].nunique())
    with col3:
        valor_total = filtered_df['valor_empresa'].sum()
        st.metric("Valor Total", f"R$ {valor_total:,.2f}")
    with col4:
        media_servicos = len(filtered_df) / filtered_df['tecnico'].nunique() if filtered_df['tecnico'].nunique() > 0 else 0
        st.metric("Média de Serviços por Técnico", f"{media_servicos:.1f}")

    # Gráficos
    st.subheader("Análise Temporal")
    # Análise temporal de serviços
    daily_services = filtered_df.groupby(filtered_df['data_execucao'].dt.date).size().reset_index()
    daily_services.columns = ['data', 'quantidade']

    fig_temporal = px.line(daily_services, x='data', y='quantidade',
                        title='Evolução Diária de Serviços')
    st.plotly_chart(fig_temporal, use_container_width=True)

    # Machine Learning - Clustering de Técnicos
    if len(filtered_df) > 0:
        st.subheader("Análise de Performance dos Técnicos")

        # Preparar dados para clustering
        tech_metrics = filtered_df.groupby('tecnico').agg({
            'id': 'count',
            'valor_tecnico': 'mean',
            'valor_empresa': 'mean'
        }).reset_index()

        if len(tech_metrics) >= 3:  # Precisamos de pelo menos 3 técnicos para fazer clustering
            # Normalizar dados
            scaler = StandardScaler()
            X = scaler.fit_transform(tech_metrics[['id', 'valor_tecnico', 'valor_empresa']])

            # Aplicar K-means
            n_clusters = min(3, len(tech_metrics))
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            tech_metrics['cluster'] = kmeans.fit_predict(X)

            # Criar gráfico de dispersão 3D
            fig_3d = px.scatter_3d(tech_metrics, 
                                x='id', y='valor_tecnico', z='valor_empresa',
                                color='cluster', text='tecnico',
                                title='Clustering de Técnicos por Performance',
                                labels={'id': 'Quantidade de Serviços',
                                        'valor_tecnico': 'Valor Médio por Técnico',
                                        'valor_empresa': 'Valor Médio para Empresa'})
            st.plotly_chart(fig_3d, use_container_width=True)
        else:
            st.info("Dados insuficientes para análise de clustering (necessário pelo menos 3 técnicos)")

    # Mapa de calor
    st.subheader("Distribuição Geográfica dos Serviços")
    # Filtrar coordenadas válidas
    map_df = filtered_df[filtered_df['latitude'].notna() & filtered_df['longitude'].notna()]

    if len(map_df) > 0:
        # Criar mapa
        m = folium.Map(location=[map_df['latitude'].mean(), map_df['longitude'].mean()], 
                    zoom_start=12)

        # Adicionar heatmap
        heat_data = [[row['latitude'], row['longitude']] for index, row in map_df.iterrows()]
        folium.plugins.HeatMap(heat_data).add_to(m)

        # Exibir mapa
        folium_static(m)
    else:
        st.info("Sem dados geográficos disponíveis para o período selecionado")

    # Análise por tipo de serviço
    if len(filtered_df) > 0:
        st.subheader("Distribuição por Tipo de Serviço")
        service_type_dist = filtered_df['tipo_servico'].value_counts()

        fig_pie = px.pie(values=service_type_dist.values, 
                        names=service_type_dist.index,
                        title='Distribuição de Tipos de Serviço')
        st.plotly_chart(fig_pie, use_container_width=True)

        # Análise de status
        st.subheader("Status dos Serviços")
        status_dist = filtered_df['status'].value_counts()

        fig_bar = px.bar(x=status_dist.index, y=status_dist.values,
                        title='Distribuição de Status',
                        labels={'x': 'Status', 'y': 'Quantidade'})
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Sem dados disponíveis para o período selecionado")

except Exception as e:
    st.error(f" Erro inesperado: {str(e)}")
    st.info("Por favor, recarregue a página. Se o erro persistir, verifique os logs do Railway.")
