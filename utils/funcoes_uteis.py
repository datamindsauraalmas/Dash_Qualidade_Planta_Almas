import os
import pandas as pd
import pandas as pd
from dotenv import load_dotenv
from zoneinfo import ZoneInfo
from supabase import create_client
import numpy as np

# Carregamento das credenciais do ambiente
load_dotenv()


# =======================================
# Função para leitura de arquivo .parquet
# =======================================
def ler_parquet(caminho_arquivo: str) -> pd.DataFrame:
    if not os.path.exists(caminho_arquivo):
        raise FileNotFoundError(f"Arquivo .parquet não encontrado: {caminho_arquivo}")

    df = pd.read_parquet(caminho_arquivo, engine="pyarrow")
    return df


# ==================================================================================
# Função para tratar do fuso horário Brasil‑São Paulo antes de enviar ao supabase
# ==================================================================================
tz_br = ZoneInfo("America/Sao_Paulo")

def preparar_df(df, ts_cols):
    for col in ts_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')  # transforma em datetime
        if df[col].dt.tz is None:  # apenas se não tiver timezone
            df[col] = df[col].dt.tz_localize(
                tz_br, ambiguous='infer', nonexistent='shift_forward'
            )
        df[col] = df[col].dt.tz_convert("UTC")  # converte para UTC
    return df

# =========================================
# Função para enviar os dados ao supabase
# =========================================
# Função de envio com processamento em blocos e serialização de datetime com fuso
def enviar_dados_supabase(df, table_name, url, key, chunk_size=500):
    supabase = create_client(url, key)

    # Serialização robusta
    def serializar_valor(valor):
        if pd.isna(valor):  # Trata NaN, NaT, None
            return None
        if isinstance(valor, pd.Timestamp):
            return valor.isoformat()
        if isinstance(valor, (np.integer, np.floating)):
            return valor.item()
        if isinstance(valor, float) and (np.isnan(valor) or np.isinf(valor)):
            return None
        return valor

    # Etapa 1: substitui NaNs e aplica serialização
    df_serializado = df.apply(lambda col: col.map(serializar_valor))

    # Etapa 2: transforma em registros validados
    registros = []
    for _, row in df_serializado.iterrows():
        registro = {}
        for col, val in row.items():
            try:
                # Conversão final defensiva
                if isinstance(val, float) and (np.isnan(val) or np.isinf(val)):
                    registro[col] = None
                else:
                    registro[col] = val
            except Exception:
                registro[col] = None
        registros.append(registro)

    # Limpa a tabela
    supabase.table(table_name).delete().neq("id", 0).execute()

    # Insere em blocos
    resposta = None
    for i in range(0, len(registros), chunk_size):
        batch = registros[i:i+chunk_size]
        resposta = supabase.table(table_name).insert(batch).execute()
    return resposta