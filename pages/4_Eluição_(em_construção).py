import streamlit as st
import os
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv
from supabase import create_client, Client
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
from zoneinfo import ZoneInfo  # TZ São Paulo

# === Configurações iniciais ===
st.set_page_config(layout="wide", page_title="Análise por Batelada - Eluição", page_icon="🧪")
st.title("🧪 Análise Comparativa por Batelada - Eluição")

# Auto-refresh: 15 minutos = 900.000 ms
st_autorefresh(interval=15 * 60 * 1000, key="auto_refresh_15min")

# === Sidebar: Recarregar manual ===
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

    # Normalização DataHoraReal: ISO8601 -> tz-aware UTC -> TZ São Paulo -> tz-naive
    if "DataHoraReal" in df.columns and not df.empty:
        df["DataHoraReal"] = (
            pd.to_datetime(df["DataHoraReal"], utc=True, errors="coerce")
              .dt.tz_convert(TZ_SP)
              .dt.tz_localize(None)  # horário local já aplicado
        )
    return df

# === Carregar dados (tabela das bateladas) ===
df = ler_dados_supabase("resultados_bateladas")
if df.empty:
    st.warning("Nenhum dado disponível.")
    st.stop()

# Ordenação temporal antes de cálculos
df = df.sort_values(["Fonte", "DataHoraReal"], kind="stable")

# Datas de referência (só para legenda informativa)
data_max = df["DataHoraReal"].max()
data_min_total = df["DataHoraReal"].min()

# === Sidebar — Filtros ===
st.sidebar.header("Filtros")

# RESET: limpar chaves desta página antes dos widgets e fazer rerun
if st.sidebar.button("🔄 Resetar Filtros"):
    for k in ["fontes_Eluicao", "periodo_bat_v1", "periodo_movel_bat", "grafico_unico_bat", "bat_range_bat"]:
        st.session_state.pop(k, None)
    st.experimental_rerun()

# 1) Fontes (Eluição) — usa a lista fornecida
fontes_Eluicao = [
    "CUBA_Entrada_Au", "CUBA_Saida_Au", "CUBA_Entrada_NaOH", "CUBA_Entrada_CN", "ELU_Rica",
    "ELU_Pobre", "CUBA_Saida_NaOH", "CUBA_Saida_CN", "ELU_ATV"
]
# Interseção com o que existe nos dados
fontes_disponiveis = sorted(set(df["Fonte"].dropna().unique()).intersection(fontes_Eluicao))
if not fontes_disponiveis:
    st.warning("Nenhuma das fontes de Eluição está presente nos dados.")
    st.stop()

fontes_default = [f for f in st.session_state.get("fontes_Eluicao", fontes_disponiveis) if f in fontes_disponiveis]
fontes_sel = st.sidebar.multiselect(
    "Fontes (Eluição):",
    fontes_disponiveis,
    default=fontes_default,
    key="fontes_Eluicao"
)

# 2) Período (SEM min/max) — padrão = [hoje-30d, hoje] no fuso SP
hoje_sp = datetime.now(TZ_SP).date()
inicio_padrao = (datetime.now(TZ_SP) - timedelta(days=30)).date()

periodo_default = st.session_state.get("periodo_bat_v1", [inicio_padrao, hoje_sp])
if not (isinstance(periodo_default, (list, tuple)) and len(periodo_default) == 2):
    periodo_default = [inicio_padrao, hoje_sp]

periodo = st.sidebar.date_input(
    "Período:",
    value=periodo_default,
    key="periodo_bat_v1"
)
# Não escrever em st.session_state["periodo_bat_v1"] após o widget.

# Normaliza retorno (pode vir data única)
if isinstance(periodo, (list, tuple)) and len(periodo) == 2:
    inicio, fim = periodo
else:
    inicio = fim = periodo

# 3) Intervalo de Bateladas
bateladas_disponiveis = sorted(
    map(int, pd.to_numeric(df["Batelada"], errors="coerce").dropna().astype(int).unique())
)
if not bateladas_disponiveis:
    st.warning("Sem valores de Batelada válidos para filtrar.")
    st.stop()

bat_min, bat_max = int(min(bateladas_disponiveis)), int(max(bateladas_disponiveis))
bat_default = st.session_state.get("bat_range_bat", (bat_min, bat_max))
if not (isinstance(bat_default, (list, tuple)) and len(bat_default) == 2):
    bat_default = (bat_min, bat_max)

bat_range = st.sidebar.slider(
    "Intervalo de Bateladas:",
    min_value=bat_min,
    max_value=bat_max,
    value=(int(bat_default[0]), int(bat_default[1])),
    key="bat_range_bat"
)

# 4) Média móvel e gráfico único
periodo_movel_val = st.session_state.get("periodo_movel_bat", 6)
periodo_movel = st.sidebar.slider(
    "Média Móvel (períodos):", 1, 20, value=periodo_movel_val, key="periodo_movel_bat"
)
grafico_unico_val = st.session_state.get("grafico_unico_bat", True)
grafico_unico = st.sidebar.checkbox(
    "Exibir em gráfico único", value=grafico_unico_val, key="grafico_unico_bat"
)

# Legenda informativa com o range de datas presente nos dados
if pd.notna(data_min_total) and pd.notna(data_max):
    st.sidebar.caption(f"Intervalo nos dados: {data_min_total.date()} a {data_max.date()}")

# === Aplicar filtros ===
df_f = df[
    (df["Fonte"].isin(fontes_sel)) &
    (pd.to_numeric(df["Batelada"], errors="coerce").astype("Int64").between(bat_range[0], bat_range[1])) &
    (df["DataHoraReal"].dt.date >= inicio) &
    (df["DataHoraReal"].dt.date <= fim)
].copy()

if df_f.empty:
    st.warning("Nenhum registro encontrado com os filtros selecionados.")
    st.stop()

# Ordena de novo após filtro
df_f = df_f.sort_values(["Fonte", "DataHoraReal"], kind="stable")

# === Média móvel por Fonte ===
df_f["MediaMovel"] = (
    df_f
    .groupby("Fonte", group_keys=False)
    .apply(lambda g: g.assign(
        MediaMovel=g["Valor"].rolling(window=st.session_state["periodo_movel_bat"], min_periods=1).mean()
    ))
)["MediaMovel"]

# === Visualização ===
if st.session_state["grafico_unico_bat"]:
    # Gráfico único (Plotly Express para hover com Batelada)
    fig = px.line(
        df_f,
        x="DataHoraReal",
        y="MediaMovel",
        color="Fonte",
        markers=False,
        title="Comparativo por Data (Média Móvel)",
        hover_data=["Batelada", "Valor"]
    )
    fig.update_layout(
        xaxis_title="Data e Hora",
        yaxis_title="Valor (Média Móvel)",
        height=600
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    # Um gráfico por Fonte (bruto + média móvel)
    for fonte in sorted(df_f["Fonte"].unique()):
        dados_fonte = df_f[df_f["Fonte"] == fonte]
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dados_fonte["DataHoraReal"], y=dados_fonte["Valor"],
            mode="markers", name="Bruto",
            marker=dict(size=4)
        ))
        fig.add_trace(go.Scatter(
            x=dados_fonte["DataHoraReal"], y=dados_fonte["MediaMovel"],
            mode="lines", name="Média Móvel"
        ))
        fig.update_layout(
            title=fonte,
            xaxis_title="Data e Hora",
            yaxis_title="Valor",
            height=500
        )
        st.subheader(fonte)
        st.plotly_chart(fig, use_container_width=True)

# === Tabela detalhada ===
with st.expander("🔍 Ver tabela de dados"):
    st.dataframe(
        df_f.sort_values(["Fonte", "Batelada", "DataHoraReal"]),
        use_container_width=True
    )