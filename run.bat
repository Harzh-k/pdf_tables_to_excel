@echo off
echo Starting PDF to Excel Converter...
echo.

if not exist "venv" (
    echo [ERROR] Virtual environment not found. Run setup.bat first.
    pause
    exit /b 1
)

call venv\Scripts\activate.bat
streamlit run app.py --server.port 8501

pause
