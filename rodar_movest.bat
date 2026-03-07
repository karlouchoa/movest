@echo off
setlocal

cd /d "%~dp0"

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
"venv\Scripts\python.exe" -m pip install --disable-pip-version-check --quiet pandas sqlalchemy pyodbc
if errorlevel 1 (
  echo Falha ao instalar dependencias.
  pause
  exit /b 1
)

echo [4/4] Executando aplicacao...
set LOG_FILE=execucao_movest.log
"venv\Scripts\python.exe" main.py >> "%LOG_FILE%" 2>&1
set EXIT_CODE=%ERRORLEVEL%

if %EXIT_CODE% neq 0 (
  echo Execucao finalizada com erro. Veja o log: %LOG_FILE%
  pause
  exit /b %EXIT_CODE%
)

echo Execucao finalizada com sucesso.
echo Log salvo em: %LOG_FILE%
pause
exit /b 0
