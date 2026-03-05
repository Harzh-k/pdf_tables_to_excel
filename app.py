"""
PDF → Excel Converter — Streamlit Application

A production-grade tool for converting complex financial PDF tables
into structured Excel spreadsheets with zero data loss.
"""

from __future__ import annotations

import logging
import sys
import tempfile
import time
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st
import base64

# ── Ensure project root is on sys.path ────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.config import OUTPUT_DIR, UPLOAD_DIR
from backend.extractor.pdf_engine import PDFExtractor
from backend.extractor.table_reconstructor import TableReconstructor
from backend.extractor.excel_writer import ExcelWriter
from backend.models import ExtractionResult

# ── Logging setup ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(name)s │ %(levelname)s │ %(message)s",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════
#  Helper functions
# ══════════════════════════════════════════════════════════════════════════

def get_base64_image(image_path):
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode()

# ══════════════════════════════════════════════════════════════════════════
#  Page configuration & custom CSS
# ══════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Drag-n-fly | Bajaj Life",
    page_icon="✈️",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# Initialize Session State
if 'is_processed' not in st.session_state:
    st.session_state.is_processed = False
if 'extraction_result' not in st.session_state:
    st.session_state.extraction_result = None
if 'output_path' not in st.session_state:
    st.session_state.output_path = None
if 'elapsed_time' not in st.session_state:
    st.session_state.elapsed_time = 0

# Custom CSS for a high-end production look (Compact)
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');

    /* ── Global Styles ────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'Outfit', -apple-system, BlinkMacSystemFont, sans-serif;
        overflow: hidden !important; /* Prevent scrolling */
    }

    .stApp {
        background-color: #FFFFFF;
        background-image: radial-gradient(at 0% 0%, rgba(37, 99, 235, 0.02) 0, transparent 50%), 
                          radial-gradient(at 50% 0%, rgba(37, 99, 235, 0.03) 0, transparent 50%);
        height: 100vh;
    }

    /* ── Header Branding ─────────────────────────────── */
    .brand-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        text-align: center;
        padding-top: 0.5rem;
        margin-bottom: 1rem;
    }

    .brand-title {
        font-size: 2.8rem;
        font-weight: 800;
        background: linear-gradient(90deg, #003399 0%, #0055ff 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-top: -8px;
        letter-spacing: -0.04em;
    }

    .brand-subtext {
        color: #64748B;
        font-size: 0.8rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        margin-top: -4px;
    }

    /* ── Upload Box ──────────────────────────────────── */
    .upload-container {
        background: #F8FAFC;
        border: 2px dashed #E2E8F0;
        border-radius: 20px;
        padding: 1.5rem 1rem;
        text-align: center;
        transition: all 0.3s ease;
        margin-bottom: 1rem;
    }

    /* ── Action Buttons ───────────────────────────────── */
    .stDownloadButton > button, .stButton > button {
        width: 100% !important;
        background: #003399 !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.8rem !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        box-shadow: 0 4px 6px -1px rgba(0, 51, 153, 0.2) !important;
        transition: all 0.2s ease !important;
    }

    .stButton > button:hover {
        background: #002266 !important;
        transform: translateY(-1px);
    }

    /* ── Metric Cards ─────────────────────────────────── */
    [data-testid="stMetric"] {
        background: #F8FAFC;
        border: 1px solid #F1F5F9;
        padding: 0.75rem;
        border-radius: 12px;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 0.75rem !important;
    }
    
    [data-testid="metric-container"] > div {
        font-size: 1.25rem !important;
    }

    /* ── Footer ───────────────────────────────────────── */
    .footer-minimal {
        text-align: center;
        color: #94A3B8;
        font-size: 0.75rem;
        margin-top: 1rem;
        padding-top: 0.75rem;
        border-top: 1px solid #F1F5F9;
    }

    /* ── Streamlit overrides ────────────────────────────── */
    .stFileUploader {
        margin-bottom: 0 !important;
    }
    .stProgress { margin-top: 1rem; }
    
    #MainMenu, footer, header {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)


# ══════════════════════════════════════════════════════════════════════════
#  Header / Branding
# ══════════════════════════════════════════════════════════════════════════

logo_b64 = get_base64_image("bajaj-life-logo.png")

st.markdown(
    f"""
    <div class="brand-container">
        <img src="data:image/png;base64,{logo_b64}" width="180">
        <div class="brand-title">Drag-n-fly</div>
        <div class="brand-subtext">powered by Bajaj Life Gen AI Workbench</div>
    </div>
    """,
    unsafe_allow_html=True,
)


# ══════════════════════════════════════════════════════════════════════════
#  Main Frame
# ══════════════════════════════════════════════════════════════════════════

main_container = st.container()

with main_container:
    uploaded_file = st.file_uploader(
        "Upload PDF",
        type=["pdf"],
        label_visibility="collapsed",
    )

    # Check if a new file is uploaded (reset state)
    if uploaded_file:
        if 'last_uploaded' not in st.session_state or st.session_state.last_uploaded != uploaded_file.name:
            st.session_state.is_processed = False
            st.session_state.last_uploaded = uploaded_file.name

    if not uploaded_file:
        st.markdown(
            """
            <div class="upload-container">
                <div style="font-size: 2.5rem; margin-bottom: 0.5rem;">📁</div>
                <div style="font-size: 1rem; font-weight: 600; color: #1E293B;">Click or Drag PDF here</div>
                <div style="color: #64748B; font-size: 0.75rem;">Optimized for Financial Statements</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    
    # SHOW PROCESS BUTTON IF FILE UPLOADED BUT NOT PROCESSED
    elif not st.session_state.is_processed:
        st.markdown(f"<p style='text-align: center; color: #64748B; font-size: 0.85rem;'>Selected: <b>{uploaded_file.name}</b></p>", unsafe_allow_html=True)
        
        button_placeholder = st.empty()
        if button_placeholder.button("🚀 Start Extraction"):
            button_placeholder.empty() # Remove button immediately
            
            progress_bar = st.progress(0, text="Initializing Engine...")
            status_text = st.empty()
            start_time = time.time()
            
            pdf_bytes = uploaded_file.getvalue()
            pdf_path = UPLOAD_DIR / uploaded_file.name
            pdf_path.write_bytes(pdf_bytes)
            
            try:
                def update_progress(current, total):
                    pct = int((current / total) * 70) # First 70% for extraction
                    progress_bar.progress(pct, text=f"🔍 Extracting Tables...")
                    status_text.markdown(f"<p style='text-align: center; color: #003399; font-weight: 600; font-size: 0.9rem;'>Processing Page {current} of {total}</p>", unsafe_allow_html=True)

                extractor = PDFExtractor(pdf_path)
                result = extractor.extract(progress_callback=update_progress)
                
                status_text.markdown("<p style='text-align: center; color: #003399; font-weight: 600; font-size: 0.9rem;'>🔧 Reconstructing Structure...</p>", unsafe_allow_html=True)
                progress_bar.progress(85, text="🔧 Reconstructing...")
                
                if result.tables:
                    reconstructor = TableReconstructor()
                    result.tables = reconstructor.reconstruct(result.tables)
                    
                    if result.tables:
                        progress_bar.progress(95, text="📊 Generating Excel...")
                        output_path = OUTPUT_DIR / (pdf_path.stem + ".xlsx")
                        writer = ExcelWriter()
                        writer.write(result, output_path)
                        
                        st.session_state.extraction_result = result
                        st.session_state.output_path = output_path
                        st.session_state.is_processed = True
                        st.session_state.elapsed_time = time.time() - start_time
                        
                progress_bar.progress(100, text="Success!")
                time.sleep(0.5)
                progress_bar.empty()
                status_text.empty()
                st.rerun() 
                
            except Exception as exc:
                st.error(f"Error: {exc}")
            finally:
                if pdf_path.exists(): pdf_path.unlink()

    # SHOW RESULTS IF PROCESSED
    if st.session_state.is_processed and st.session_state.extraction_result:
        result = st.session_state.extraction_result
        output_path = st.session_state.output_path
        
        # Results Container (Stable transition)
        results_box = st.container()
        with results_box:
            st.markdown("<div style='background: #F0F9FF; border-radius: 16px; padding: 1.25rem; border: 1px solid #BAE6FD;'>", unsafe_allow_html=True)
            
            # Micro Summary
            m1, m2, m3 = st.columns(3)
            with m1: st.metric("Tables", len(result.tables))
            with m2: 
                rows = sum(t.total_rows for t in result.tables)
                st.metric("Rows", rows)
            with m3:
                acc = sum(t.confidence for t in result.tables)/len(result.tables)*100 if result.tables else 0
                st.metric("Accuracy", f"{acc:.0f}%")
    
            st.markdown("<br>", unsafe_allow_html=True)
    
            if output_path and output_path.exists():
                with open(output_path, "rb") as f:
                    btn_data = f.read()
                st.download_button(
                    label=f"📥 Download {output_path.name}",
                    data=btn_data,
                    file_name=output_path.name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
                st.markdown(f"<p style='text-align: center; color: #64748B; font-size: 0.75rem; margin-top: 5px;'>Finished in {st.session_state.elapsed_time:.1f}s</p>", unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            if st.button("🔄 Reset / Process New File"):
                st.session_state.is_processed = False
                st.session_state.extraction_result = None
                st.rerun()


# ══════════════════════════════════════════════════════════════════════════
#  Footer
# ══════════════════════════════════════════════════════════════════════════

st.markdown(
    """
    <div class="footer-minimal">
        © 2026 Bajaj Life Insurance · Internal Gen AI Tool
    </div>
    """,
    unsafe_allow_html=True,
)
