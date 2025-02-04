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
from concurrent.futures import ThreadPoolExecutor
import threading

# Configuração da página
st.set_page_config(
    page_title="Dashboard de Serviços",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configurações de cache e performance
st.cache_data.clear()

# Função para conectar ao banco de dados
def get_connection():
    return psycopg2.connect(
        "postgresql://postgres:fvPCqIuJkOHZxVtzPgmYbiYDbikhylXa@roundhouse.proxy.rlwy.net:10419/railway"
    )

# Função para executar queries de forma segura
def execute_query(query, params=None):
    conn = None
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Erro ao executar query: {str(e)}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

# Definir as bases por categoria
BASES_INSTALACAO = [
    'BASE BAURU',
    'BASE BOTUCATU',
    'BASE CAMPINAS',
    'BASE LIMEIRA',
    'BASE PAULINIA',
    'BASE PIRACICABA',
    'BASE RIBEIRAO PRETO',
    'BASE SAO JOSE DO RIO PRETO',
    'BASE SOROCABA',
    'BASE SUMARE',
    'GPON BAURU',
    'GPON RIBEIRAO PRETO'
]

BASES_MANUTENCAO = [
    'BASE ARARAS VT',
    'BASE BOTUCATU VT',
    'BASE MDU ARARAS',
    'BASE MDU BAURU',
    'BASE MDU MOGI',
    'BASE MDU PIRACICABA',
    'BASE MDU SJRP',
    'BASE PIRACICABA VT',
    'BASE RIBEIRÃO VT',
    'BASE SERTAOZINHO VT',
    'BASE SUMARE VT',
    'BASE VAR BAURU',
    'BASE VAR PIRACICABA',
    'BASE VAR SUMARE'
]

BASES_DESCONEXAO = [
    'DESCONEXAO',
    'DESCONEXÃO BOTUCATU',
    'DESCONEXÃO CAMPINAS',
    'DESCONEXAO RIBEIRAO PRETO'
]

# Função para carregar bases disponíveis
@st.cache_data(ttl=3600)
def load_bases(tipo_base='INSTALACAO'):
    try:
        if tipo_base == 'INSTALACAO':
            bases = BASES_INSTALACAO
        elif tipo_base == 'MANUTENCAO':
            bases = BASES_MANUTENCAO
        elif tipo_base == 'DESCONEXAO':
            bases = BASES_DESCONEXAO
        else:
            bases = BASES_INSTALACAO + BASES_MANUTENCAO + BASES_DESCONEXAO
        
        return ['Todas'] + sorted(bases)
    except Exception as e:
        st.error(f"Erro ao carregar bases: {str(e)}")
        return ['Todas']

# Função para carregar dados
@st.cache_data(ttl=3600)
def load_data(start_date=None, end_date=None, base=None):
    try:
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
        
        params = []
        if start_date and end_date:
            query += " AND os.data_execucao BETWEEN %s AND %s"
            params.extend([start_date, end_date])
        if base and base != 'Todas':
            query += " AND b.nome = %s"
            params.append(base)

        return execute_query(query, params)

    except Exception as e:
        st.error(f"Erro ao carregar dados: {str(e)}")
        return pd.DataFrame()

# Função para carregar datas disponíveis
@st.cache_data(ttl=3600)
def load_date_range():
    try:
        query = """
        SELECT MIN(data_execucao) as min_date, MAX(data_execucao) as max_date
        FROM ordens_servico
        WHERE data_execucao IS NOT NULL
        """
        df = execute_query(query)
        if df.empty:
            return datetime.now() - timedelta(days=30), datetime.now()
        return df['min_date'].iloc[0], df['max_date'].iloc[0]
    except Exception as e:
        st.error(f"Erro ao carregar datas: {str(e)}")
        return datetime.now() - timedelta(days=30), datetime.now()

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

    # Filtro de tipo de base
    tipo_base = st.sidebar.selectbox(
        'Tipo de Base',
        ['INSTALACAO', 'MANUTENCAO', 'DESCONEXAO', 'TODAS'],
        index=0  # Começa com INSTALACAO selecionado
    )

    # Filtro de base
    bases = load_bases(tipo_base)
    selected_base = st.sidebar.selectbox('Base', bases)

    # Carregar dados filtrados
    with st.spinner('Carregando dados...'):
        df = load_data(date_range[0], date_range[1], selected_base)
        
        # Filtrar por tipo de base se não for 'Todas'
        if tipo_base != 'TODAS' and selected_base == 'Todas':
            if tipo_base == 'INSTALACAO':
                df = df[df['base'].isin(BASES_INSTALACAO)]
            elif tipo_base == 'MANUTENCAO':
                df = df[df['base'].isin(BASES_MANUTENCAO)]
            elif tipo_base == 'DESCONEXAO':
                df = df[df['base'].isin(BASES_DESCONEXAO)]

    # Layout principal
    st.title("Dashboard de Análise de Serviços")
    st.success(f"Total de registros carregados: {len(df):,}")

    # Métricas principais em abas
    tab1, tab2 = st.tabs([" Métricas", " Gráficos"])
    
    with tab1:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total de Serviços", f"{len(df):,}")
        with col2:
            st.metric("Total de Técnicos", f"{df['tecnico'].nunique():,}")
        with col3:
            valor_total = df['valor_empresa'].sum()
            st.metric("Valor Total", f"R$ {valor_total:,.2f}")
        with col4:
            media_servicos = len(df) / df['tecnico'].nunique() if df['tecnico'].nunique() > 0 else 0
            st.metric("Média de Serviços por Técnico", f"{media_servicos:,.1f}")

    with tab2:
        # Gráficos em abas
        chart_tab1, chart_tab2, chart_tab3 = st.tabs([" Temporal", " Mapa", " Distribuição"])
        
        with chart_tab1:
            # Análise temporal de serviços com agregação otimizada
            daily_services = df.set_index('data_execucao').resample('D').size().reset_index()
            daily_services.columns = ['data', 'quantidade']

            fig_temporal = px.line(daily_services, x='data', y='quantidade',
                                title='Evolução Diária de Serviços')
            st.plotly_chart(fig_temporal, use_container_width=True)

        with chart_tab2:
            # Mapa de calor com clustering para melhor performance
            st.subheader("Distribuição Geográfica dos Serviços")
            map_df = df[df['latitude'].notna() & df['longitude'].notna()]

            if len(map_df) > 0:
                # Usar K-means para reduzir pontos se necessário
                if len(map_df) > 1000:
                    coords = map_df[['latitude', 'longitude']].values
                    kmeans = KMeans(n_clusters=1000, random_state=42)
                    clusters = kmeans.fit(coords)
                    centers = clusters.cluster_centers_
                    
                    m = folium.Map(location=[centers[:, 0].mean(), centers[:, 1].mean()],
                                zoom_start=12)
                    
                    heat_data = [[row[0], row[1]] for row in centers]
                else:
                    m = folium.Map(location=[map_df['latitude'].mean(), map_df['longitude'].mean()],
                                zoom_start=12)
                    
                    heat_data = [[row['latitude'], row['longitude']] for _, row in map_df.iterrows()]
                
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
