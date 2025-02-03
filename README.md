# Dashboard de Análise de Serviços

Dashboard interativo para análise de serviços técnicos, construído com Streamlit e Machine Learning.

## Funcionalidades

- Análise temporal de serviços
- Clustering de técnicos por performance (Machine Learning)
- Mapa de calor com distribuição geográfica
- Análise de tipos de serviço e status
- Filtros interativos por data e base

## Tecnologias Utilizadas

- Python
- Streamlit
- PostgreSQL
- Scikit-learn
- Plotly
- Folium

## Requisitos

- Python 3.8+
- PostgreSQL
- Dependências listadas em `requirements.txt`

## Configuração Local

1. Clone o repositório:
```bash
git clone https://github.com/seu-usuario/ProjetoDashBasic.git
cd ProjetoDashBasic
```

2. Instale as dependências:
```bash
pip install -r requirements.txt
```

3. Configure as variáveis de ambiente:
Crie um arquivo `.env` com:
```
DATABASE_URL=postgresql://usuario:senha@host:porta/banco
```

4. Execute a aplicação:
```bash
streamlit run dashboard.py
```

## Deploy no Railway

1. Faça fork deste repositório
2. No Railway:
   - Crie um novo projeto
   - Escolha "Deploy from GitHub repo"
   - Conecte com seu repositório
   - Adicione as variáveis de ambiente necessárias
   - O deploy será automático

## Estrutura do Projeto

```
ProjetoDashBasic/
├── dashboard.py         # Aplicação principal
├── process_data_fast.py # Processamento de dados
├── create_tables.py     # Criação das tabelas
├── requirements.txt     # Dependências
├── Procfile            # Configuração para deploy
└── README.md           # Documentação
```

## Variáveis de Ambiente

- `DATABASE_URL`: URL de conexão com o banco PostgreSQL

## Contribuindo

1. Faça um Fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request
