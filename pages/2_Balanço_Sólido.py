import streamlit as st
import os
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
from streamlit_autorefresh import st_autorefresh
from zoneinfo import ZoneInfo  # TZ correto para SP

# === Configurações iniciais ===
st.set_page_config(layout="wide", page_title="Médias Móveis - Sólidas", page_icon="⛏️")
st.title("⛏️ Visualizador de Séries Temporais - Sólidas")

# 15 minutos = 900.000 ms
st_autorefresh(interval=15 * 60 * 1000, key="auto_refresh_15min")

# Sidebar: recarregar manual
if st.sidebar.button("🔁 Recarregar Dados"):
    st.cache_data.clear()
    st.session_state.hash_parquet = None
    st.toast("📦 Dados recarregados manualmente!")

# === Supabase & TZ ===
# Carrega o .env da pasta atual
load_dotenv()

def get_config(key: str, default: str | None = None) -> str | None:
    """
    Busca um valor de configuração na seguinte ordem:
    1) st.secrets (para Streamlit Cloud / secrets.toml)
    2) Variáveis de ambiente (para uso com .env + python-dotenv)
    3) default (se nada encontrado)
    """
    # 1) Tenta st.secrets, mas sem quebrar se não houver secrets.toml
    try:
        if key in st.secrets:
            return st.secrets[key]
    except FileNotFoundError:
        # Nenhum secrets.toml definido → ignora e segue
        pass

    # 2) Tenta variável de ambiente
    value = os.getenv(key)
    if value is not None:
        return value

    # 3) Fallback
    return default

SUPABASE_URL = get_config("SUPABASE_URL")
SUPABASE_KEY = get_config("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("Configuração de Supabase ausente. Verifique .env (local) ou Secrets (Streamlit Cloud).")
    st.stop()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
TZ_SP = ZoneInfo("America/Sao_Paulo")

# === Loader com paginação e normalização de TZ ===
@st.cache_data(show_spinner=True, ttl=900)
def ler_dados_supabase(tabela: str, pagina_tamanho: int = 1000) -> pd.DataFrame:
    offset = 0
    dados_completos = []
    while True:
        resposta = (
            supabase
            .table(tabela)
            .select("*")
            .range(offset, offset + pagina_tamanho - 1)
            .execute()
        )
        dados = resposta.data
        if not dados:
            break
        dados_completos.extend(dados)
        offset += pagina_tamanho

    df = pd.DataFrame(dados_completos)

    # Normaliza DataHoraReal: ISO8601 -> tz-aware UTC -> TZ São Paulo -> tz-naive
    if "DataHoraReal" in df.columns and not df.empty:
        df["DataHoraReal"] = (
            pd.to_datetime(df["DataHoraReal"], utc=True, errors="coerce")
              .dt.tz_convert(TZ_SP)
              .dt.tz_localize(None)
        )
    return df

# === Carrega dados e aplica filtro fixo para fontes sólidas ===
df = ler_dados_supabase("resultados_analiticos")
fontes_s = ["LIX_Au_S", "TQ2_Au_S", "TQ5_Au_S", "TQ6_Au_S", "REJ_Au_S", "TQ9_Au_S", "TQ10_Au_S", "TQ11_Au_S", "TQ12_Au_S"]
df = df[df["Fonte"].isin(fontes_s)]

if df.empty:
    st.warning("Nenhum dado disponível para as fontes sólidas.")
    st.stop()

# Ordenação temporal antes de cálculos
df = df.sort_values(["Fonte", "DataHoraReal"], kind="stable")

# === Datas padrão (independentes do intervalo dos dados) ===
hoje_sp = datetime.now(TZ_SP).date()
inicio_padrao = (datetime.now(TZ_SP) - timedelta(days=30)).date()

# === Sidebar ===
st.sidebar.header("Configurações")

# RESET precisa acontecer antes da criação do widget para não tocar no session_state depois
if st.sidebar.button("🔄 Resetar Filtros"):
    # limpa chaves usadas POR ESTE ARQUIVO
    for k in ["fontes_solidos", "periodo_solidos_v3", "periodo_movel_solidos", "grafico_unico_solidos"]:
        st.session_state.pop(k, None)
    st.experimental_rerun()

# Fontes disponíveis e multiselect
fontes_disponiveis = sorted(df["Fonte"].unique())
fontes_default = [f for f in st.session_state.get("fontes_solidos", fontes_s) if f in fontes_disponiveis]
fontes_sel = st.sidebar.multiselect(
    "Fontes:", fontes_disponiveis, default=fontes_default, key="fontes_solidos"
)

# Seletor de período: SEM min/max; default = [hoje-30d, hoje]; key nova
periodo_default = st.session_state.get("periodo_solidos_v3", [inicio_padrao, hoje_sp])
# Garante que o default é uma dupla [ini, fim]
if not (isinstance(periodo_default, (list, tuple)) and len(periodo_default) == 2):
    periodo_default = [inicio_padrao, hoje_sp]

periodo = st.sidebar.date_input(
    "Período:",
    value=periodo_default,
    key="periodo_solidos_v3"
)
# NUNCA escreveremos em st.session_state["periodo_solidos_v3"] depois daqui.

# Normaliza retorno do widget (pode vir data única)
if isinstance(periodo, (list, tuple)) and len(periodo) == 2:
    inicio, fim = periodo
else:
    inicio = fim = periodo

# Controles restantes (podem ter value= seguro)
periodo_movel_val = st.session_state.get("periodo_movel_solidos", 6)
periodo_movel = st.sidebar.slider(
    "Média Móvel (períodos):", 1, 20, value=periodo_movel_val, key="periodo_movel_solidos"
)
grafico_unico_val = st.session_state.get("grafico_unico_solidos", True)
grafico_unico = st.sidebar.checkbox(
    "Exibir em gráfico único", value=grafico_unico_val, key="grafico_unico_solidos"
)

# === Filtra dados pelo período/seleção ===
df_filtrado = df[
    (df["Fonte"].isin(fontes_sel)) &
    (df["DataHoraReal"].dt.date >= inicio) &
    (df["DataHoraReal"].dt.date <= fim)
].copy()

if df_filtrado.empty:
    st.warning("Nenhum dado encontrado para o período ou fontes selecionadas.")
    st.stop()

# === Calcula média móvel (respeitando ordem temporal) ===
df_filtrado = df_filtrado.sort_values(["Fonte", "DataHoraReal"], kind="stable")
df_filtrado["MediaMovel"] = (
    df_filtrado
    .groupby("Fonte", group_keys=False)
    .apply(lambda g: g.assign(
        MediaMovel=g["Valor"].rolling(window=st.session_state["periodo_movel_solidos"], min_periods=1).mean()
    ))
)["MediaMovel"]

# === Ordem lógica dos gráficos ===
ordem_manual = fontes_s
fontes_sel = sorted(fontes_sel, key=lambda f: ordem_manual.index(f) if f in ordem_manual else len(ordem_manual))

# === Exibição ===
if st.session_state["grafico_unico_solidos"]:
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
        title=f"Médias Móveis - {st.session_state['periodo_movel_solidos']} períodos",
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
            marker=dict(size=4)
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