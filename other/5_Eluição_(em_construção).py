import streamlit as st
import pandas as pd
import plotly.express as px
from utils.utils import carregar_dados_batelada

st.set_page_config(layout="wide", page_title="Análise por Batelada", page_icon="🧪")
st.title("🧪 Análise Comparativa por Batelada")

# Carregar dados com Batelada
df = carregar_dados_batelada()

if df.empty:
    st.warning("Nenhum dado disponível.")
    st.stop()

# Sidebar: filtros
st.sidebar.header("Filtros")

fontes_disponiveis = sorted(df["Fonte"].unique())
fontes_sel = st.sidebar.multiselect("Fontes:", fontes_disponiveis, default=fontes_disponiveis)

df = df[df["Fonte"].isin(fontes_sel)]

bateladas_disponiveis = sorted(df["Batelada"].unique())
bat_min, bat_max = min(bateladas_disponiveis), max(bateladas_disponiveis)
bat_range = st.sidebar.slider("Intervalo de Bateladas:", min_value=int(bat_min), max_value=int(bat_max), value=(int(bat_min), int(bat_max)))

df = df[(df["Batelada"] >= bat_range[0]) & (df["Batelada"] <= bat_range[1])]

if df.empty:
    st.warning("Nenhuma batelada encontrada com os filtros selecionados.")
    st.stop()

# === Limitar datas entre 2023 e hoje
df = df[(df["DataHoraReal"] >= "2023-07-22") & (df["DataHoraReal"] <= pd.Timestamp.today())]

# Gráfico: Data no eixo X e Batelada como tooltip
fig = px.line(
    df,
    x="DataHoraReal",
    y="Valor",
    color="Fonte",
    markers=True,
    title="Comparativo de Valor por Data (Bateladas no Hover)",
    hover_data=["Batelada"]
)
fig.update_layout(
    xaxis_title="Data e Hora",
    yaxis_title="Valor",
    height=600
)
st.plotly_chart(fig, use_container_width=True)


# Detalhes em tabela
with st.expander("🔍 Ver tabela de dados"):
    st.dataframe(df.sort_values(["Fonte", "Batelada"]))
