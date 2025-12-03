#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
import pandas as pd

from dotenv import load_dotenv


# In[2]:


# Modulo especifico para rodar o notebook fora da raiz do projeto
# Garante que a raiz do projeto (onde está a pasta utils/) entre no sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..")) \
    if "__file__" in globals() else os.path.abspath("..")

if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# Adiciona pasta raiz ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# In[3]:


# Carrega as variáveis de ambiente definidas no arquivo .env, sobrescrevendo valores já existentes no ambiente
load_dotenv(override=True)

# Carregar variavel de saida para salvar o arquivo parquet
SUPABASE_TABELA_RESULTADOS_ANALITICOS = os.getenv("SUPABASE_TABELA_RESULTADOS_ANALITICOS")
SUPABASE_TABELA_RESULTADOS_BATELADAS = os.getenv("SUPABASE_TABELA_RESULTADOS_BATELADAS")

#Acesso Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

from utils.funcoes_uteis import ler_parquet, preparar_df, enviar_dados_supabase


# In[4]:


# Leitura dos arquivos parquet
df_resultados_analiticos = ler_parquet('../export/amostras_horarias.parquet')
df_resultados_bateladas = ler_parquet('../export/amostras_bateladas.parquet')


# In[5]:


# Preparar coluna de data com fuso horário
df_resultados_analiticos = preparar_df(df_resultados_analiticos,['DataHoraReal'])
df_resultados_bateladas= preparar_df(df_resultados_bateladas,['DataHoraReal'])


# In[6]:


df_resultados_analiticos


# In[7]:


# Enviar para o supabase
envio1 = enviar_dados_supabase(df_resultados_analiticos, SUPABASE_TABELA_RESULTADOS_ANALITICOS, SUPABASE_URL, SUPABASE_KEY)
envio2 = enviar_dados_supabase(df_resultados_bateladas, SUPABASE_TABELA_RESULTADOS_BATELADAS, SUPABASE_URL, SUPABASE_KEY)

