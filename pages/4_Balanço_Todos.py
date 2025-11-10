import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from utils.utils import carregar_dados, get_remote_hash, URL_PARQUET

# === Configurações iniciais ===
st.set_page_config(layout="wide", page_title="Médias Móveis - Sólidas", page_icon="🪨")
st.title("🪨 Visualizador de Séries Temporais - Sólidas")

# Sidebar: recarregar manual
if st.sidebar.button("🔁 Recarregar Dados"):
    st.cache_data.clear()
    st.session_state.hash_parquet = None
    st.toast("📦 Dados recarregados manualmente!")

# === Desabilitado: verificação de hash temporariamente ===
# novo_hash = get_remote_hash(URL_PARQUET)
# hash_antigo = st.session_state.get("hash_parquet")
# if novo_hash and novo_hash != hash_antigo:
#     st.cache_data.clear()
#     st.session_state.hash_parquet = novo_hash
#     st.toast("🆕 Novo conteúdo detectado!")


# === Carrega dados e aplica filtro fixo para fontes sólidas ===
df = carregar_dados()
fontes_s = ["LIX_Au_S", "TQ2_Au_S", "TQ6_Au_S", "REJ_Au_S"]
df = df[df["Fonte"].isin(fontes_s)]

if df.empty:
    st.warning("Nenhum dado disponível para as fontes sólidas.")
    st.stop()

# === Datas ===
data_max = df["DataHoraReal"].max()
data_min_total = df["DataHoraReal"].min()
data_min_default = data_max - pd.Timedelta(days=30)

# === Estado Inicial ===
st.session_state.setdefault("fontes", fontes_s.copy())
st.session_state.setdefault("periodo", [data_min_default.date(), data_max.date()])
st.session_state.setdefault("periodo_movel", 6)
st.session_state.setdefault("grafico_unico", True)

# === Sidebar ===
st.sidebar.header("Configurações")

if st.sidebar.button("🔄 Resetar Filtros"):
    st.session_state["fontes"] = fontes_s.copy()
    st.session_state["periodo"] = [data_min_default.date(), data_max.date()]
    st.session_state["periodo_movel"] = 6
    st.session_state["grafico_unico"] = True

# Corrige default do multiselect com base nos dados válidos
fontes_disponiveis = sorted(df["Fonte"].unique())
st.session_state["fontes"] = [f for f in st.session_state["fontes"] if f in fontes_disponiveis]

fontes_sel = st.sidebar.multiselect("Fontes:", fontes_disponiveis, default=st.session_state["fontes"], key="fontes")
periodo = st.sidebar.date_input("Período:", value=st.session_state["periodo"], min_value=data_min_total.date(), max_value=data_max.date(), key="periodo")
periodo_movel = st.sidebar.slider("Média Móvel (períodos):", 1, 20, value=st.session_state["periodo_movel"], key="periodo_movel")
grafico_unico = st.sidebar.checkbox("Exibir em gráfico único", value=st.session_state["grafico_unico"], key="grafico_unico")

inicio, fim = periodo if isinstance(periodo, (list, tuple)) and len(periodo) == 2 else (periodo, data_max.date())
st.sidebar.caption(f"Intervalo disponível: {data_min_total.date()} a {data_max.date()}")

# === Filtra dados ===
df_filtrado = df[
    (df["Fonte"].isin(fontes_sel)) &
    (df["DataHoraReal"].dt.date >= inicio) &
    (df["DataHoraReal"].dt.date <= fim)
].copy()

if df_filtrado.empty:
    st.warning("Nenhum dado encontrado para o período ou fontes selecionadas.")
    st.stop()

# === Calcula média móvel ===
df_filtrado["MediaMovel"] = df_filtrado.groupby("Fonte")["Valor"].transform(
    lambda x: x.rolling(window=periodo_movel, min_periods=1).mean()
)

# === Ordem lógica dos gráficos ===
ordem_manual = fontes_s
fontes_sel = sorted(fontes_sel, key=lambda f: ordem_manual.index(f) if f in ordem_manual else len(ordem_manual))

# === Exibição ===
if grafico_unico:
    fig = go.Figure()
    for fonte in fontes_sel:
        dados_fonte = df_filtrado[df_filtrado["Fonte"] == fonte]
        fig.add_trace(go.Scatter(
            x=dados_fonte["DataHoraReal"],
            y=dados_fonte["MediaMovel"],
            mode="lines",
            name=fonte
        ))
    fig.update_layout(
        title=f"Médias Móveis - {periodo_movel} períodos",
        xaxis_title="Data",
        yaxis_title="Valor",
        height=600
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    for fonte in fontes_sel:
        dados_fonte = df_filtrado[df_filtrado["Fonte"] == fonte]
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dados_fonte["DataHoraReal"],
            y=dados_fonte["Valor"],
            mode="markers",
            name="Bruto",
            marker=dict(size=4, color="lightgray")
        ))
        fig.add_trace(go.Scatter(
            x=dados_fonte["DataHoraReal"],
            y=dados_fonte["MediaMovel"],
            mode="lines",
            name="Média Móvel"
        ))
        fig.update_layout(
            title=fonte,
            xaxis_title="Data",
            yaxis_title="Valor",
            height=500
        )
        st.subheader(fonte)
        st.plotly_chart(fig, use_container_width=True)
