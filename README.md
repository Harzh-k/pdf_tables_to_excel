# 📊 PDF → Excel Converter

A production-grade tool for converting complex financial PDF tables into structured Excel spreadsheets — **with zero data loss**.

## ✨ Features

- **Dual-engine extraction** — pdfplumber (primary) + Camelot (fallback)
- **Merged header support** — multi-level headers reconstructed faithfully
- **Exact numeric precision** — financial numbers preserved as-is
- **One table per sheet** — each table gets its own titled worksheet
- **Styled output** — headers, borders, auto-fitted columns
- **Beautiful UI** — minimal white-and-blue Streamlit interface

## 🚀 Quick Start

### Prerequisites

- **Python 3.10+**
- **Ghostscript** *(optional, enables Camelot fallback engine)*
  - Download: [ghostscript.com](https://ghostscript.com/releases/gsdnld.html)

### Setup

```bash
# 1. Run setup (creates venv + installs deps)
setup.bat

# 2. Launch the app
run.bat
```

The app opens at **http://localhost:8501**

### Manual Setup

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```

## 📁 Project Structure

```
├── app.py                  # Streamlit frontend
├── setup.bat               # Windows setup
├── run.bat                 # Launch script
├── requirements.txt
├── .streamlit/
│   └── config.toml         # White & blue theme
├── backend/
│   ├── config.py           # Settings & tolerances
│   ├── models.py           # Data models
│   └── extractor/
│       ├── pdf_engine.py         # Dual-engine extraction
│       ├── table_reconstructor.py  # Header/merge logic
│       └── excel_writer.py       # openpyxl output
├── uploads/                # Runtime (auto-created)
└── outputs/                # Runtime (auto-created)
```

## 🔧 How It Works

1. **Upload** a financial PDF via the web interface
2. **pdfplumber** extracts tables using line/text detection
3. If confidence is low, **Camelot** re-extracts as a fallback
4. **Table Reconstructor** detects merged headers and cleans data
5. **Excel Writer** produces a styled `.xlsx` with exact structure

## ⚠️ Limitations

- **Scanned PDFs** (image-based) are not supported in v1
- Very unusual table layouts may need extraction tolerance tuning
- Ghostscript is required for Camelot's lattice mode

## 📄 License

MIT
