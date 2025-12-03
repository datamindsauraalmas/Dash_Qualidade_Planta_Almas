#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
import pandas as pd

from dotenv import load_dotenv


# In[2]:


# Carrega as variáveis de ambiente definidas no arquivo .env, sobrescrevendo valores já existentes no ambiente
load_dotenv(override=True)

# Carregar variavel de saida para salvar o arquivo parquet
SUPABASE_TABELA_RESULTADOS_ANALITICOS = os.getenv("SUPABASE_TABELA_RESULTADOS_ANALITICOS")
SUPABASE_TABELA_RESULTADOS_BATELADAS = os.getenv("SUPABASE_TABELA_RESULTADOS_BATELADAS")

#Acesso Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

from utils.funcoes_uteis import *
from utils.config import *


# In[3]:


# Leitura dos arquivos parquet
df_resultados_analiticos = ler_parquet(PARQUET_AMOSTRAS_HORARIAS)
df_resultados_bateladas = ler_parquet(PARQUET_AMOSTRAS_BATELADAS)


# In[4]:


# Preparar coluna de data com fuso horário
df_resultados_analiticos = preparar_df(df_resultados_analiticos,['DataHoraReal'])
df_resultados_bateladas= preparar_df(df_resultados_bateladas,['DataHoraReal'])


# In[6]:


# Enviar para o supabase
envio1 = enviar_dados_supabase(df_resultados_analiticos, SUPABASE_TABELA_RESULTADOS_ANALITICOS, SUPABASE_URL, SUPABASE_KEY)
envio2 = enviar_dados_supabase(df_resultados_bateladas, SUPABASE_TABELA_RESULTADOS_BATELADAS, SUPABASE_URL, SUPABASE_KEY)

