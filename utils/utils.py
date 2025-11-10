import pandas as pd
import hashlib
import requests
import streamlit as st

# URL pública do Parquet no Azure
URL_PARQUET = URL_PARQUET = 'data/consolidado.parquet'

# Função para verificar se o conteúdo remoto mudou
def get_remote_hash(url):
    response = requests.get(url)
    if response.status_code != 200:
        return None
    return hashlib.md5(response.content).hexdigest()

# Carregar dados com cache de 10 minutos
@st.cache_data(ttl=6000)
def carregar_dados():
    df = pd.read_parquet(URL_PARQUET, engine="pyarrow")
    df["DataHoraReal"] = pd.to_datetime(df["DataHoraReal"])
    return df


# === Fonte de dados com Batelada ===
URL_PARQUET_BATELADA = 'data/consolidado_batelada.parquet'

@st.cache_data(ttl=6000)
def carregar_dados_batelada():
    df = pd.read_parquet(URL_PARQUET_BATELADA, engine="pyarrow")
    df["DataHoraReal"] = pd.to_datetime(df["DataHoraReal"])
    return df
#