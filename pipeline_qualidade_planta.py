import subprocess
import os
import time
from datetime import datetime
from dotenv import load_dotenv
import sys
from pathlib import Path

# ==========================
# Carrega variáveis do .env
# ==========================
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# ==================
# Diretório de logs
# ==================
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

exec_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_EXECUCAO = LOG_DIR / f"pipeline_execucao_ETL_qualidade_planta{exec_timestamp}.csv"
LOG_ERROS = LOG_DIR / f"pipeline_erros_ETL_qualidade_planta{exec_timestamp}.csv"

houve_erro = False

with open(LOG_EXECUCAO, "w", encoding="utf-8") as log_file:
    log_file.write("timestamp;script;status;duracao_segundos\n")

with open(LOG_ERROS, "w", encoding="utf-8") as log:
    log.write("timestamp;script;status;duracao_segundos;stdout;stderr;excecao\n")

# =================
# Lista de scripts
# =================
SCRIPTS = [
    "export/ETL.py",
    "export/load_Supabase.py",
]

# =========================================
# Função para executar scripts
# =========================================
def executar_script(script):
    global houve_erro

    script_path = BASE_DIR / script

    nome_script = script_path.name
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n[+] Executando: {script_path}", flush=True)

    inicio = time.time()

    try:
        python_exec = sys.executable  

        # >>> AQUI: garante que a raiz do projeto esteja no PYTHONPATH
        env = os.environ.copy()
        env["PYTHONPATH"] = str(BASE_DIR) + os.pathsep + env.get("PYTHONPATH", "")

        result = subprocess.run(
            [python_exec, str(script_path)],
            stdout=subprocess.PIPE,   # para log
            stderr=subprocess.PIPE,
            text=True,
            env=env,                  # <<< usa o env modificado
        )

        fim = time.time()
        duracao = round(fim - inicio, 2)
        status = "SUCESSO" if result.returncode == 0 else "FALHA"

        with open(LOG_EXECUCAO, "a", encoding="utf-8") as log_file:
            log_file.write(f"{timestamp};{nome_script};{status};{duracao}\n")

        if result.returncode != 0:
            houve_erro = True
            with open(LOG_ERROS, "a", encoding="utf-8") as log:
                log.write(
                    f"{timestamp};{nome_script};FALHA;{duracao};"
                    f"{result.stdout.strip().replace(';','|')};"
                    f"{result.stderr.strip().replace(';','|')};\n"
                )
            print(f"[!] Falha ao executar {script_path}. Verifique {LOG_ERROS.name}", flush=True)
        else:
            print(result.stdout, flush=True)
            print(f"[✓] Sucesso: {script_path} (Tempo: {duracao}s)", flush=True)

    except Exception as e:
        fim = time.time()
        duracao = round(fim - inicio, 2)
        houve_erro = True

        with open(LOG_EXECUCAO, "a", encoding="utf-8") as log_file:
            log_file.write(f"{timestamp};{nome_script};EXCECAO;{duracao}\n")

        with open(LOG_ERROS, "a", encoding="utf-8") as log:
            log.write(
                f"{timestamp};{nome_script};EXCECAO;{duracao};;;"
                f"{str(e).replace(';','|')}\n"
            )

        print(f"[X] Exceção ao executar {script_path}. Verifique {LOG_ERROS.name}", flush=True)

# =========================================
# Main
# =========================================
if __name__ == "__main__":
    print("Iniciando pipeline Qualidade Plantae\n", flush=True)

    for script in SCRIPTS:
        executar_script(script)

    if houve_erro:
        print(f"\n[!] Execução concluída com erros. Consulte: {LOG_ERROS.name}", flush=True)
    else:
        if LOG_ERROS.exists():
            LOG_ERROS.unlink()
        print(f"\n[✓] Pipeline finalizado sem erros. Log em: {LOG_EXECUCAO.name}", flush=True)