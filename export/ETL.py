# %%
# gerar_consolidados_sem_hash_e_sem_upload.py
import pandas as pd
import requests
import os
import sys
from io import BytesIO


# %%
# Modulo especifico para rodar o notebook fora da raiz do projeto
# Garante que a raiz do projeto (onde está a pasta utils/) entre no sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..")) \
    if "__file__" in globals() else os.path.abspath("..")

if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# Adiciona pasta raiz ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# %%
# =========================
# ====== PARTE 1: Séries (consolidado.parquet)
# =========================

def carregar_dados(arquivo, aba, colunas, horas=None):
    """
    Lê uma aba do Excel com header=4 e apenas as colunas indicadas.
    Renomeia colunas para ['Data', HH:MM, HH:MM, ...].
    Preenche datas faltantes somando +1 dia quando a linha anterior tem Data.
    Mantém exatamente a mesma lógica do script original.
    """
    dados = pd.read_excel(arquivo, sheet_name=aba, header=4, usecols=colunas)
    nomes_colunas = ["Data"] + (horas if horas else [f"{str(h).zfill(2)}:00" for h in range(1, 24)] + ["24:00"])
    dados.columns = nomes_colunas
    for i in range(len(dados)):
        if pd.isna(dados.loc[i, "Data"]) and i > 0 and pd.notna(dados.loc[i-1, "Data"]):
            dados.loc[i, "Data"] = dados.loc[i-1, "Data"] + pd.Timedelta(days=1)
    return dados.dropna(subset=["Data"]).dropna(how="all")

def processar_dados(dados, valor_maximo, nome_fonte):
    """
    Varre linhas/horas, limpa valores, filtra por limites e monta:
    ['Fonte','DataHoraReal','Valor','MediaMovel_6'].
    Mantém a MM de janela 6 exatamente como estava (sem groupby/ordenar antes).
    """
    linhas = []
    for _, row in dados.iterrows():
        data_atual = row["Data"]
        for coluna in row.index:
            if coluna != "Data":
                valor_bruto = row[coluna]
                if isinstance(valor_bruto, str):
                    valor_bruto = valor_bruto.replace("<", "").replace(",", ".").strip()
                valor = pd.to_numeric(valor_bruto, errors="coerce")
                if pd.notna(valor) and valor != 0 and valor <= valor_maximo:
                    linhas.append({
                        "Data": data_atual,
                        "Hora": coluna,
                        "Valor": valor,
                        "Fonte": nome_fonte
                    })
    df = pd.DataFrame(linhas)
    if not df.empty:
        df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
        df["HoraCorrigida"] = df["Hora"].replace({"24:00": "23:59"})
        df["DataHoraReal"] = pd.to_datetime(df["Data"].astype(str) + " " + df["HoraCorrigida"], errors="coerce")
        df = df.dropna(subset=["DataHoraReal", "Valor"])
        df = df[df["Valor"] <= valor_maximo].reset_index(drop=True)
        df["MediaMovel_6"] = df["Valor"].rolling(window=6, min_periods=1).mean()
        df = df[["Fonte", "DataHoraReal", "Valor", "MediaMovel_6"]]
    return df

# =========================
# ====== PARTE 2: Batelada (consolidado_batelada.parquet)
# =========================

def carregar_dados_batelada(arquivo, aba, colunas):
    """
    Lê aba com header=4, zera nomes das colunas (0..N-1), seleciona posições em 'colunas',
    renomeia para ['Data','Batelada','Hora','ValorBruto'] e preenche Data ausente (+1 dia).
    """
    dados = pd.read_excel(arquivo, sheet_name=aba, header=4)
    dados.columns = list(range(dados.shape[1]))
    df = dados[colunas].copy()
    df.columns = ["Data", "Batelada", "Hora", "ValorBruto"]

    for i in range(1, len(df)):
        if pd.isna(df.at[i, "Data"]) and pd.notna(df.at[i - 1, "Data"]):
            try:
                df.at[i, "Data"] = df.at[i - 1, "Data"] + pd.Timedelta(days=1)
            except Exception:
                continue

    return df.dropna(subset=["Data", "Hora", "Batelada", "ValorBruto"]).dropna(how="all")

def processar_dados_batelada(dados, valor_maximo, nome_fonte):
    """
    Normaliza ValorBruto, filtra e monta:
    ['DataHoraReal','Valor','Batelada','Fonte'].
    Mantém exatamente a mesma lógica do script original.
    """
    dados["Valor"] = (
        dados["ValorBruto"]
        .astype(str)
        .str.replace(r"[^\d,.\-]", "", regex=True)
        .str.replace(",", ".", regex=False)
        .str.replace(r"\.{2,}", ".", regex=True)
        .str.strip()
    )

    dados["Valor"] = pd.to_numeric(dados["Valor"], errors="coerce")
    dados = dados[(dados["Valor"].notna()) & (dados["Valor"] != 0) & (dados["Valor"] <= valor_maximo)].copy()

    if dados.empty:
        return pd.DataFrame()

    dados["Data"] = pd.to_datetime(dados["Data"], errors="coerce").dt.date
    dados["HoraCorrigida"] = dados["Hora"].astype(str).str.strip().replace({"24:00": "23:59"})
    dados["DataHoraReal"] = pd.to_datetime(
        dados["Data"].astype(str) + " " + dados["HoraCorrigida"], errors="coerce"
    )

    dados["Batelada"] = pd.to_numeric(dados["Batelada"], errors="coerce")
    dados = dados[dados["Batelada"].notna()]
    dados = dados[dados["Batelada"] % 1 == 0]
    dados["Batelada"] = dados["Batelada"].astype("int64")
    dados = dados.dropna(subset=["DataHoraReal", "Valor", "Batelada"])
    dados["Fonte"] = nome_fonte

    return dados[["DataHoraReal", "Valor", "Batelada", "Fonte"]]

# =========================
# ====== EXECUÇÃO (sem hash e sem upload)
# =========================

def baixar_excel_para_bytesio(fonte_excel):
    """
    Se 'fonte_excel' for URL (http/https), baixa via requests.
    Se for caminho local (.xlsx), abre direto.
    Retorna um objeto BytesIO ou o próprio caminho (ambos são aceitos por read_excel).
    """
    if isinstance(fonte_excel, str) and fonte_excel.lower().startswith(("http://", "https://")):
        print("Baixando arquivo do SharePoint/URL...")
        resp = requests.get(fonte_excel)
        if resp.status_code != 200:
            raise RuntimeError(f"Erro ao baixar o arquivo (status {resp.status_code}).")
        return BytesIO(resp.content)
    # caminho local:
    return fonte_excel

def gerar_consolidados(fonte_excel):
    """
    Executa os dois pipelines (séries e batelada) SEM hash e SEM upload.
    Salva consolidado.parquet e consolidado_batelada.parquet localmente.
    Retorna (df_final, df_final_batelada).
    """
    excel_data = baixar_excel_para_bytesio(fonte_excel)

    # ----- Conjuntos (iguais ao original) -----
    horarios_3  = ["08:00", "16:00", "24:00"]
    horarios_4  = ["06:00", "12:00", "18:00", "24:00"]
    horarios_6  = ["04:00", "08:00", "12:00", "16:00", "20:00", "24:00"]
    horarios_2  = ["12:00", "24:00"]
    horarios_12 = ["02:00", "04:00", "06:00", "08:00", "10:00", "12:00", "14:00", "16:00", "18:00", "20:00", "22:00", "24:00"]
    horarios_bar = ["04:00", "08:00", "12:00", "16:00", "20:00", "24:00"]

    conjuntos = [
        # Sólidas
        ("Sólidas", [0, 30, 35, 40], 50,  "LIX_Au_S", horarios_3),
        ("Sólidas", [0, 45, 59],     50,  "LIX_Au_S", horarios_2),
        ("Sólidas", [0, 27, 32, 37, 42], 50,  "LIX_Au_S", horarios_4),
        ("Sólidas", [0, 47, 49, 51, 53, 55, 57], 50, "LIX_Au_S", horarios_6),
        ("Sólidas", [0, 31, 36, 41], 200, "LIX_PX",    horarios_3),
        ("Sólidas", [0, 46, 61],     200, "LIX_PX",    horarios_2),
        ("Sólidas", [0, 48, 50, 52, 54, 56, 58], 200, "LIX_PX", horarios_6),

        ("Sólidas", [0, 76, 77, 78], 50,  "REJ_Au_S",  horarios_3),
        ("Sólidas", [0, 72, 74],     50,  "REJ_Au_S",  horarios_2),

        ("Sólidas Saída TQ02, TQ05 e TQ06", [0, 7, 8, 9],  50, "TQ2_Au_S", horarios_3),
        ("Sólidas Saída TQ02, TQ05 e TQ06", [0, 14, 15, 16], 50, "TQ5_Au_S", horarios_3),
        ("Sólidas Saída TQ02, TQ05 e TQ06", [0, 21, 22, 23], 50, "TQ6_Au_S", horarios_3),

        ("Carvão TQ Produção", [0, 8], 50, "TQ2_Au_S", ["12:00"]),

        # Líquidas
        ("Água de Processo", [0, 15, 16, 17, 18, 19, 20], 0.6, "BAR_Au_L",  horarios_bar),
        ("Líquidas",         [0, 38, 39, 40],            50,   "LIX_Au_L",  horarios_3),
        ("Líquidas Saída TQ1 TQ2 TQ6 TQ7", [0] + list(range(7, 31)), 5, "TQ01_Au_L", None),
        ("Líquidas Saída TQ1 TQ2 TQ6 TQ7", [0, 32, 33, 34], 1.5, "TQ02_Au_L", horarios_3),
        ("Líquidas Saída TQ1 TQ2 TQ6 TQ7", [0, 51, 52, 53], 50,  "TQ06_Au_L", horarios_3),
        ("Líquidas Saída TQ1 TQ2 TQ6 TQ7", [0] + list(range(82, 94)), 50, "TQ07_Au_L", horarios_12),
        ("Líquidas", [0, 101, 102, 103], 0.8, "REJ_Au_L", horarios_3),
    ]

    todos_dados = []
    print("Processando dados (séries)...")
    for aba, colunas, val_max, nome, horas in conjuntos:
        dados = carregar_dados(excel_data, aba, colunas, horas)
        df = processar_dados(dados, val_max, nome)
        if not df.empty:
            todos_dados.append(df)

    df_final = pd.concat(todos_dados, ignore_index=True) if todos_dados else pd.DataFrame(
        columns=["Fonte", "DataHoraReal", "Valor", "MediaMovel_6"]
    )
    df_final = df_final.sort_values(by="DataHoraReal", ascending=False).reset_index(drop=True)
    print(f"Séries consolidadas: {len(df_final)} linhas")
    df_final.to_parquet("consolidado.parquet", index=False)
    print("Arquivo salvo: consolidado.parquet")

    # ----- Conjuntos de batelada (iguais ao original) -----
    conjuntos_batelada = [
        ("Cuba Principal",    [1, 4, 3, 5],   500,  "CUBA_Entrada_Au"),
        ("Cuba Principal",    [1, 4, 3, 6],   500,  "CUBA_Entrada_NaOH"),
        ("Cuba Principal",    [1, 4, 3, 7],   500,  "CUBA_Entrada_CN"),
        ("Cuba Principal",    [9, 12, 11, 13], 500, "CUBA_Saida_Au"),
        ("Cuba Principal",    [9, 12, 11, 51], 500, "CUBA_Saida_NaOH"),
        ("Cuba Principal",    [9, 12, 11, 52], 500, "CUBA_Saida_CN"),
        ("Acacia",  [1, 4, 2, 5],  5000,  "ACA_Rica"),
        ("Acacia",  [1, 4, 2, 11], 5000,  "ACA_Pobre"),
        ("Acacia",  [1, 4, 2, 7],  5000,  "ACA_CN"),
        ("Eluição - Carvão", [2, 1, 3, 4],  5000, "ELU_Rica"),
        ("Eluição - Carvão", [6, 1, 7, 8],  5000, "ELU_Pobre"),
        ("Eluição - Carvão", [6, 1, 7, 11], 5000, "ELU_ATV"),
    ]

    todos_batelada = []
    print("Processando dados de batelada...")
    for aba, colunas, val_max, nome in conjuntos_batelada:
        dados_b = carregar_dados_batelada(excel_data, aba, colunas)
        df_b = processar_dados_batelada(dados_b, val_max, nome)
        print(f"{nome}: {len(df_b)} linhas processadas")
        if not df_b.empty:
            todos_batelada.append(df_b)

    df_final_batelada = (
        pd.concat(todos_batelada, ignore_index=True).drop_duplicates(
            subset=["Fonte", "DataHoraReal", "Valor", "Batelada"]
        ).reset_index(drop=True)
        if todos_batelada else
        pd.DataFrame(columns=["DataHoraReal", "Valor", "Batelada", "Fonte"])
    )
    df_final_batelada = df_final_batelada.sort_values(by="DataHoraReal", ascending=False)
    df_final_batelada["Valor"] = pd.to_numeric(df_final_batelada["Valor"], errors="coerce")
    if not df_final_batelada.empty:
        df_final_batelada["Batelada"] = df_final_batelada["Batelada"].astype("int64")

    df_final_batelada.to_parquet("consolidado_batelada.parquet", index=False, engine="pyarrow", compression="snappy")
    print("Arquivo salvo: consolidado_batelada.parquet")

    return df_final, df_final_batelada



# %%
if __name__ == "__main__":
    URL_EXCEL = r"C:\Users\Dataminds\Aura Minerals\Almas - Performance - Data Minds - Data Minds\01 - Planta\Projeto - Dash_Qualidade\almasdash\data\Resultados Planta.xlsx"

    df_final, df_final_batelada = gerar_consolidados(URL_EXCEL)
    print("Pronto: Parquets locais gerados sem hash e sem upload.")

# %%
df_final

# %%
df_final_batelada


