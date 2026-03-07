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

echo [4/4] Executando aplicacao...
"venv\Scripts\python.exe" main.py
if errorlevel 1 (
  echo A aplicacao foi encerrada com erro.
  pause
  exit /b 1
)

echo Aplicacao finalizada.
pause
exit /b 0
