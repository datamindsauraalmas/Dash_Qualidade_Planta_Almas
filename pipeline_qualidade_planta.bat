@echo off
setlocal

REM Caminho absoluto do ambiente virtual
set VENV_PATH=C:\ScriptsDatamindsPIP\Dash_Qualidade\venv

REM Caminho absoluto do script principal
set SCRIPT_PATH=C:\ScriptsDatamindsPIP\Dash_Qualidade\pipeline_qualidade_planta.py

REM Ativa o ambiente virtual
call "%VENV_PATH%\Scripts\activate.bat"

REM Move para o diretório onde está o script
cd /d C:\ScriptsDatamindsPIP\Dash_Qualidade

REM Executa o script principal
python "%SCRIPT_PATH%"

endlocal