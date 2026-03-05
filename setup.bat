@echo off
echo ============================================
echo   PDF to Excel Converter - Setup
echo ============================================
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed or not in PATH.
    echo         Install Python 3.10+ from https://python.org
    pause
    exit /b 1
)

echo [1/3] Creating virtual environment...
if not exist "venv" (
    python -m venv venv
    echo       Virtual environment created.
) else (
    echo       Virtual environment already exists.
)

echo [2/3] Installing dependencies...
call venv\Scripts\activate.bat
pip install -r requirements.txt --quiet
echo       Dependencies installed.

echo [3/3] Checking Ghostscript (required for Camelot)...
gswin64c --version >nul 2>&1
if errorlevel 1 (
    gswin32c --version >nul 2>&1
    if errorlevel 1 (
        echo.
        echo [WARNING] Ghostscript not found!
        echo           Camelot fallback engine requires Ghostscript.
        echo           Download from: https://ghostscript.com/releases/gsdnld.html
        echo           The app will still work with pdfplumber as the primary engine.
        echo.
    ) else (
        echo       Ghostscript found (32-bit).
    )
) else (
    echo       Ghostscript found (64-bit).
)

echo.
echo ============================================
echo   Setup complete! Run the app with:
echo   run.bat
echo ============================================
pause
