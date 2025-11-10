# === Estrutura de um app Streamlit com múltiplas páginas ===

# Arquivo: main.py
import streamlit as st

st.set_page_config(
    page_title="Dashboard Principal",
    layout="wide",
    page_icon="📊"
)

st.title("📊 Dashboard Principal")
st.markdown("Escolha uma página à esquerda para visualizar os gráficos.")


