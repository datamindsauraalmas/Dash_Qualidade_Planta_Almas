# utils/config.py
import os
from pathlib import Path
from dotenv import load_dotenv
from .paths import ROOT

# Carrega variáveis do .env localizado na raiz
load_dotenv(ROOT / ".env")


def p(var_name: str) -> Path:
    """
    Converte uma variável do .env em Path absoluto.

    - Se o valor for relativo, considera relativo à raiz do projeto (ROOT).
    - Se for um caminho absoluto (C:\\..., D:\\..., \\servidor\\pasta), usa como está.
    """
    value = os.getenv(var_name)
    if not value:
        raise KeyError(f"Variável {var_name} não definida no .env")

    path = Path(value)

    # Já é absoluto? Usa direto.
    if path.is_absolute():
        return path

    # Senão, faz relativo à raiz do projeto
    return (ROOT / path).resolve()

#==========================================================
# Acessos Arquivos da planta e mina Programa e Realizado
#==========================================================
PARQUET_AMOSTRAS_BATELADAS = p("PARQUET_AMOSTRAS_BATELADAS")
PARQUET_AMOSTRAS_HORARIAS = p("PARQUET_AMOSTRAS_HORARIAS")
URL_EXCEL = p("URL_EXCEL")