import subprocess
import os
import time
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis do .env
load_dotenv()

# Diretório de logs
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Timestamp único para esta execução
exec_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Caminhos dos arquivos de log (todos em .csv agora)
LOG_EXECUCAO = os.path.join(LOG_DIR, f"pipeline_execucao_{exec_timestamp}.csv")
LOG_ERROS = os.path.join(LOG_DIR, f"pipeline_erros_{exec_timestamp}.csv")

# Flag de controle
houve_erro = False

# Cabeçalho dos logs
with open(LOG_EXECUCAO, "w", encoding="utf-8") as log_file:
    log_file.write("timestamp;script;status;duracao_segundos\n")

with open(LOG_ERROS, "w", encoding="utf-8") as log:
    log.write("timestamp;script;status;duracao_segundos;stdout;stderr;excecao\n")

# Lista de scripts
SCRIPTS = [
    "export/ETL.py",
    "export/load_Supabase.py",
]

def executar_script(script_path):
    global houve_erro

    nome_script = os.path.basename(script_path)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n[+] Executando: {script_path}")

    try:
        python_exec = os.path.join(os.environ["VIRTUAL_ENV"], "Scripts", "python.exe")

        inicio = time.time()
        result = subprocess.run(
            [python_exec, script_path],
            capture_output=True,
            text=True,
            check=False
        )
        fim = time.time()
        duracao = round(fim - inicio, 2)

        status = "SUCESSO" if result.returncode == 0 else "FALHA"

        # Registro no log de execução
        with open(LOG_EXECUCAO, "a", encoding="utf-8") as log_file:
            log_file.write(f"{timestamp};{nome_script};{status};{duracao}\n")

        if result.returncode != 0:
            houve_erro = True
            with open(LOG_ERROS, "a", encoding="utf-8") as log:
                log.write(f"{timestamp};{nome_script};FALHA;{duracao};"
                          f"{result.stdout.strip().replace(';', '|')};"
                          f"{result.stderr.strip().replace(';', '|')};\n")

            print(f"[!] Falha ao executar {script_path}. Verifique {os.path.basename(LOG_ERROS)}")
        else:
            print(f"[✓] Sucesso: {script_path} (Tempo: {duracao}s)")

    except Exception as e:
        fim = time.time()
        duracao = round(fim - inicio, 2)
        houve_erro = True

        # Log de execução
        with open(LOG_EXECUCAO, "a", encoding="utf-8") as log_file:
            log_file.write(f"{timestamp};{nome_script};EXCECAO;{duracao}\n")

        # Log de erro
        with open(LOG_ERROS, "a", encoding="utf-8") as log:
            log.write(f"{timestamp};{nome_script};EXCECAO;{duracao};;;{str(e).replace(';', '|')}\n")

        print(f"[X] Exceção ao executar {script_path}. Verifique {os.path.basename(LOG_ERROS)}")

if __name__ == "__main__":
    print("Iniciando pipeline de dados Qualidade Planta\n")

    for script in SCRIPTS:
        executar_script(script)

    if houve_erro:
        print(f"\n[!] Execução concluída com erros. Consulte: {os.path.basename(LOG_ERROS)}")
    else:
        if os.path.exists(LOG_ERROS):
            os.remove(LOG_ERROS)
        print(f"\n[✓] Pipeline finalizado sem erros. Log em: {os.path.basename(LOG_EXECUCAO)}")