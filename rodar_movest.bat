@echo off
setlocal

cd /d "%~dp0"

set "SCRIPT_ALVO="

echo [1/4] Verificando ambiente Python...
where python >nul 2>&1
if errorlevel 1 (
  echo Python nao encontrado no PATH.
  echo Instale Python 3 e tente novamente.
  pause
  exit /b 1
)

echo [2/4] Preparando venv...
if not exist "venv\Scripts\python.exe" (
  python -m venv venv
  if errorlevel 1 (
    echo Falha ao criar venv.
    pause
    exit /b 1
  )
)

echo [3/4] Instalando dependencias...
if exist "requirements.txt" (
  "venv\Scripts\python.exe" -m pip install --disable-pip-version-check --quiet -r requirements.txt
) else (
  "venv\Scripts\python.exe" -m pip install --disable-pip-version-check --quiet -r requiriments.txt
)
if errorlevel 1 (
  echo Falha ao instalar dependencias.
  pause
  exit /b 1
)

echo [4/4] Escolhendo rotina...
echo.
echo   1 - Processar movimentacoes e atualizar T_SALDOIT
echo   2 - Auditar T_MOVEST
echo   3 - Auditar T_SALDOIT a partir da copia de seguranca
echo.
set /p OPCAO="Informe a opcao desejada [1]: "
if not defined OPCAO set "OPCAO=1"

if "%OPCAO%"=="1" set "SCRIPT_ALVO=main.py"
if "%OPCAO%"=="2" set "SCRIPT_ALVO=auditar_movest.py"
if "%OPCAO%"=="3" set "SCRIPT_ALVO=auditar_saldoit.py"

if not defined SCRIPT_ALVO (
  echo Opcao invalida: %OPCAO%
  pause
  exit /b 1
)

echo Executando %SCRIPT_ALVO%...
"venv\Scripts\python.exe" "%SCRIPT_ALVO%"
if errorlevel 1 (
  echo A aplicacao foi encerrada com erro.
  pause
  exit /b 1
)

echo Aplicacao finalizada.
pause
exit /b 0
