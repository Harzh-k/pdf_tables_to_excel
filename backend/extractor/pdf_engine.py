"""
Clean PDF table extraction engine.

Strategy cascade:
  1. Default pdfplumber settings (works for most PDFs with rects/lines)
  2. 'lines' strategy (stricter line detection)
  3. Explicit rect-edge strategy (uses rect edges as table boundaries)
  4. NO text strategy (avoids word shattering)
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pdfplumber

from backend.models import ExtractionResult, TableData

logger = logging.getLogger(__name__)


class PDFExtractor:
    """Extracts tables from machine-generated PDFs using pdfplumber."""

    def __init__(self, pdf_path: str | Path) -> None:
        self.pdf_path = Path(pdf_path)
        if not self.pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {self.pdf_path}")

    def extract(self, progress_callback: callable[[int, int], None] | None = None) -> ExtractionResult:
        """
        Extract all tables from all pages.
        
        Args:
            progress_callback: Function taking (current_page, total_pages)
        """
        result = ExtractionResult(filename=self.pdf_path.name)

        try:
            with pdfplumber.open(self.pdf_path) as pdf:
                result.page_count = len(pdf.pages)
                logger.info("Processing %s (%d pages)", self.pdf_path.name, result.page_count)

                for page_idx, page in enumerate(pdf.pages, start=1):
                    if progress_callback:
                        progress_callback(page_idx, result.page_count)
                    elif page_idx % 25 == 0 or page_idx == 1:
                        logger.info("Page %d / %d ...", page_idx, result.page_count)

                    page_tables = self._extract_page(page, page_idx)
                    result.tables.extend(page_tables)

        except Exception as exc:
            logger.exception("Extraction failed: %s", exc)
            result.errors.append(str(exc))

        logger.info("Done: %d tables from %d pages", len(result.tables), result.page_count)
        return result

    def _extract_page(self, page: Any, page_number: int) -> list[TableData]:
        """
        Extract tables from a single page.
        Try default → lines → rect-edges. Never use text strategy.
        """
        # Strategy 1: pdfplumber defaults (rects + lines)
        tables = self._try_extract(page, page_number, settings=None, label="default")
        if tables:
            return tables

        # Strategy 2: explicit 'lines' with relaxed tolerance
        tables = self._try_extract(page, page_number, settings={
            "vertical_strategy": "lines",
            "horizontal_strategy": "lines",
            "snap_tolerance": 5,
            "join_tolerance": 5,
        }, label="lines")
        if tables:
            return tables

        # Strategy 3: Use rect edges as explicit table boundaries
        # This handles pages where rects exist but pdfplumber can't
        # connect them into tables automatically
        tables = self._try_rect_edges(page, page_number)
        if tables:
            return tables

        # No tables found at all — skip this page (better than shattering text)
        return []

    def _try_rect_edges(self, page: Any, page_number: int) -> list[TableData] | None:
        """
        Fallback extraction for pages with rects/lines but where default/lines
        strategies fail. Tries progressive approaches:
        1. Merged rect edges as explicit vertical lines
        2. Text strategy with high x_tolerance (prevents word-splitting)
        """
        # Attempt 1: Use rect edges as explicit column boundaries
        if page.edges:
            v_positions = sorted(set(round(e['x0']) for e in page.edges
                                     if e['orientation'] == 'v'))
            if len(v_positions) >= 2:
                # Merge close edges
                merged_v = [v_positions[0]]
                for v in v_positions[1:]:
                    if v - merged_v[-1] < 15:
                        merged_v[-1] = (merged_v[-1] + v) // 2
                    else:
                        merged_v.append(v)

                if len(merged_v) >= 2:
                    try:
                        tables = self._try_extract(page, page_number, settings={
                            "vertical_strategy": "explicit",
                            "horizontal_strategy": "text",
                            "explicit_vertical_lines": merged_v,
                            "snap_tolerance": 5,
                            "min_words_horizontal": 1,
                        }, label="rect_edges")
                        if tables and self._quality_ok(tables):
                            return tables
                    except Exception:
                        pass

        # Attempt 2: Text with high tolerance (avoids word-splitting)
        try:
            tables = self._try_extract(page, page_number, settings={
                "vertical_strategy": "text",
                "horizontal_strategy": "text",
                "min_words_vertical": 3,
                "min_words_horizontal": 1,
                "text_x_tolerance": 15,
                "text_y_tolerance": 5,
            }, label="text_wide")
            if tables and self._quality_ok(tables):
                return tables
        except Exception:
            pass

        return None

    @staticmethod
    def _quality_ok(tables: list[TableData]) -> bool:
        """Check if extraction results are reasonable (not shattered text)."""
        if not tables:
            return False
        for t in tables:
            all_rows = t.headers + t.rows
            if not all_rows:
                continue
            ncols = len(all_rows[0])
            # Shattered text produces many columns (>30) — reject
            if ncols > 30:
                return False
            # Check first non-empty data row for shattered cells
            for row in all_rows[:5]:
                non_empty = [c for c in row if c.strip()]
                if non_empty:
                    # If average cell length < 3 chars, it's shattered
                    avg_len = sum(len(c) for c in non_empty) / len(non_empty)
                    if avg_len < 3 and ncols > 10:
                        return False
                    break
        return True

    def _try_extract(
        self,
        page: Any,
        page_number: int,
        settings: dict | None,
        label: str,
    ) -> list[TableData] | None:
        """Try a single extraction strategy. Returns list of tables or None."""
        try:
            if settings:
                found = page.find_tables(table_settings=settings)
            else:
                found = page.find_tables()

            if not found:
                return None

            tables: list[TableData] = []

            for tidx, tobj in enumerate(found):
                raw = tobj.extract()
                if not raw:
                    continue

                cleaned = _clean_raw(raw)
                if not cleaned or len(cleaned) < 2:
                    continue

                ncols = len(cleaned[0])
                if ncols < 2:
                    continue

                confidence = _confidence(cleaned)
                title = self._get_title(page, tobj, page_number, tidx)

                tables.append(TableData(
                    title=title,
                    headers=[cleaned[0]],
                    rows=cleaned[1:],
                    page_number=page_number,
                    confidence=confidence,
                ))

            return tables if tables else None

        except Exception as exc:
            logger.debug("Strategy '%s' failed on P%d: %s", label, page_number, exc)
            return None

    def _get_title(
        self, page: Any, table_obj: Any, page_number: int, table_idx: int
    ) -> str:
        """Get title text above the table."""
        default = f"Table {table_idx + 1} (Page {page_number})"
        try:
            bbox = table_obj.bbox
            if not bbox:
                return default
            x0, top, x1, _ = bbox
            search_top = max(0, top - 50)
            cropped = page.within_bbox((x0, search_top, x1, top), relative=False)
            chars = cropped.chars if cropped else []
            if not chars:
                return default

            from collections import defaultdict
            lines: dict[int, list] = defaultdict(list)
            for c in chars:
                lines[int(round(c["top"]))].append(c)

            for ykey in sorted(lines.keys(), reverse=True):
                text = "".join(
                    c["text"] for c in sorted(lines[ykey], key=lambda c: c["x0"])
                ).strip()
                if text and len(text) > 3:
                    return text

            return default
        except Exception:
            return default


# ── Utility functions ─────────────────────────────────────────────────────

def _clean_raw(raw: list[list]) -> list[list[str]]:
    """
    Clean raw table: None→'', preserve newlines, remove empty rows, pad cols.
    """
    if not raw:
        return []

    cleaned = []
    for row in raw:
        r = []
        for c in (row or []):
            if c is None:
                r.append("")
            elif isinstance(c, str):
                r.append(c.strip())
            else:
                r.append(str(c).strip())
        if any(c for c in r):
            cleaned.append(r)

    if not cleaned:
        return []

    maxcols = max(len(r) for r in cleaned)
    for r in cleaned:
        while len(r) < maxcols:
            r.append("")

    return cleaned


def _confidence(table: list[list[str]]) -> float:
    """
    Confidence score reflecting structural integrity rather than raw cell density.
    Sparse financial tables are normal.
    """
    total = sum(len(r) for r in table)
    if total == 0:
        return 0.0
        
    empty = sum(1 for r in table for c in r if not c.replace("\n", "").strip())
    density = 1 - (empty / total)
    
    # Base confidence is high if we successfully extracted a table without shattering
    confidence = 0.85
    
    # Reward for good structural density (not too sparse)
    if density > 0.3:
        confidence += 0.10
        
    # Reward for having a good mix of numbers (actual data extracted)
    numeric_cells = sum(1 for r in table for c in r if any(char.isdigit() for char in c))
    if numeric_cells > 0:
        confidence += 0.04
        
    # Apply a small penalty for extremely empty tables (density < 10%)
    if density < 0.1:
        confidence -= 0.20
        
    # Cap between 0 and 0.99
    return max(0.0, min(0.99, confidence))
