@echo off
setlocal

cd /d "%~dp0"

set "VENV_DIR=%CD%\.venv"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
set "PIP_EXE=%VENV_DIR%\Scripts\pip.exe"
set "URL=http://127.0.0.1:8000"

echo [1/4] Checking virtual environment...
if not exist "%PYTHON_EXE%" (
  echo Creating virtual environment...
  python -m venv "%VENV_DIR%"
  if errorlevel 1 (
    echo Failed to create virtual environment. Please make sure Python is installed.
    pause
    exit /b 1
  )
)

echo [2/4] Installing dependencies...
"%PYTHON_EXE%" -m pip install --upgrade pip
if errorlevel 1 (
  echo Failed to upgrade pip.
  pause
  exit /b 1
)

"%PIP_EXE%" install -r requirements.txt
if errorlevel 1 (
  echo Failed to install dependencies.
  pause
  exit /b 1
)

echo [3/4] Starting web app...
start "Git Work Report Automation" cmd /k ""%PYTHON_EXE%" -m uvicorn app.main:app --host 127.0.0.1 --port 8000"

echo [4/4] Opening browser...
powershell -NoProfile -Command "Start-Sleep -Seconds 3; Start-Process '%URL%'"

echo App started. If the browser does not open, visit %URL%
exit /b 0
