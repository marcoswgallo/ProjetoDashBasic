import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import psycopg2
from datetime import datetime, timedelta
import folium
from streamlit_folium import folium_static

# Configuração da página
st.set_page_config(
    page_title="Dashboard de Serviços",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configurações de cache
st.cache_data.clear()

# Função para conectar ao banco de dados
@st.cache_resource(ttl=3600)
def get_connection():
    database_url = "postgresql://postgres:fvPCqIuJkOHZxVtzPgmYbiYDbikhylXa@roundhouse.proxy.rlwy.net:10419/railway"
    return psycopg2.connect(database_url)

# Função para carregar dados
@st.cache_data(ttl=3600)
def load_data(start_date=None, end_date=None, base=None, limit=10000):
    try:
        conn = get_connection()
        
        # Query base
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
        
        # Adicionar filtros
        params = []
        if start_date and end_date:
            query += " AND os.data_execucao BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        if base and base != 'Todas':
            query += " AND b.nome = %s"
            params.append(base)
            
        # Adicionar limite
        query += " LIMIT %s"
        params.append(limit)
        
        df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame()

# Função para carregar datas disponíveis
@st.cache_data(ttl=3600)
def load_date_range():
    try:
        conn = get_connection()
        query = """
        SELECT MIN(data_execucao) as min_date, MAX(data_execucao) as max_date
        FROM ordens_servico
        WHERE data_execucao IS NOT NULL
        """
        df = pd.read_sql(query, conn)
        return df['min_date'].iloc[0], df['max_date'].iloc[0]
    except Exception as e:
        st.error(f"Erro ao carregar datas: {str(e)}")
        return datetime.now() - timedelta(days=30), datetime.now()

# Função para carregar bases disponíveis
@st.cache_data(ttl=3600)
def load_bases():
    try:
        conn = get_connection()
        query = "SELECT nome FROM bases ORDER BY nome"
        df = pd.read_sql(query, conn)
        return ['Todas'] + df['nome'].tolist()
    except Exception as e:
        st.error(f"Erro ao carregar bases: {str(e)}")
        return ['Todas']

try:
    # Sidebar
    st.sidebar.title("Filtros")

    # Carregar datas disponíveis
    min_date, max_date = load_date_range()
    
    # Filtro de data
    date_range = st.sidebar.date_input(
        "Período",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # Filtro de base
    bases = load_bases()
    selected_base = st.sidebar.selectbox('Base', bases)

    # Limite de registros
    limit = st.sidebar.slider('Limite de registros', 1000, 50000, 10000, step=1000)

    # Carregar dados filtrados
    df = load_data(date_range[0], date_range[1], selected_base, limit)

    # Layout principal
    st.title("Dashboard de Análise de Serviços")
    st.info(f"Mostrando {len(df)} registros dos {limit} solicitados")

    # Métricas principais em abas
    tab1, tab2 = st.tabs([" Métricas", " Gráficos"])
    
    with tab1:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total de Serviços", len(df))
        with col2:
            st.metric("Total de Técnicos", df['tecnico'].nunique())
        with col3:
            valor_total = df['valor_empresa'].sum()
            st.metric("Valor Total", f"R$ {valor_total:,.2f}")
        with col4:
            media_servicos = len(df) / df['tecnico'].nunique() if df['tecnico'].nunique() > 0 else 0
            st.metric("Média de Serviços por Técnico", f"{media_servicos:.1f}")

    with tab2:
        # Gráficos em abas
        chart_tab1, chart_tab2, chart_tab3 = st.tabs([" Temporal", " Mapa", " Distribuição"])
        
        with chart_tab1:
            # Análise temporal de serviços
            daily_services = df.groupby(df['data_execucao'].dt.date).size().reset_index()
            daily_services.columns = ['data', 'quantidade']

            fig_temporal = px.line(daily_services, x='data', y='quantidade',
                                title='Evolução Diária de Serviços')
            st.plotly_chart(fig_temporal, use_container_width=True)

        with chart_tab2:
            # Mapa de calor
            st.subheader("Distribuição Geográfica dos Serviços")
            map_df = df[df['latitude'].notna() & df['longitude'].notna()]

            if len(map_df) > 0:
                m = folium.Map(location=[map_df['latitude'].mean(), map_df['longitude'].mean()], 
                            zoom_start=12)
                heat_data = [[row['latitude'], row['longitude']] for index, row in map_df.iterrows()]
                folium.plugins.HeatMap(heat_data).add_to(m)
                folium_static(m)
            else:
                st.info("Sem dados geográficos disponíveis para o período selecionado")

        with chart_tab3:
            if len(df) > 0:
                col1, col2 = st.columns(2)
                
                with col1:
                    # Análise por tipo de serviço
                    service_type_dist = df['tipo_servico'].value_counts()
                    fig_pie = px.pie(values=service_type_dist.values, 
                                names=service_type_dist.index,
                                title='Distribuição de Tipos de Serviço')
                    st.plotly_chart(fig_pie, use_container_width=True)
                
                with col2:
                    # Análise de status
                    status_dist = df['status'].value_counts()
                    fig_bar = px.bar(x=status_dist.index, y=status_dist.values,
                                title='Distribuição de Status',
                                labels={'x': 'Status', 'y': 'Quantidade'})
                    st.plotly_chart(fig_bar, use_container_width=True)

except Exception as e:
    st.error(f"Erro inesperado: {str(e)}")
    st.info("Por favor, recarregue a página. Se o erro persistir, entre em contato com o suporte.")
