"""
Data models for the extraction pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class CellData:
    """Represents a single cell in a table."""

    value: str
    row: int
    col: int
    rowspan: int = 1
    colspan: int = 1
    is_header: bool = False
    is_merged: bool = False

    @property
    def numeric_value(self) -> float | None:
        """
        Attempt to parse the cell value as a number.
        Handles commas, parentheses for negatives, percentage signs, etc.
        Returns None if the value is not numeric.
        """
        if not self.value or not self.value.strip():
            return None

        text = self.value.strip()

        # Remove common formatting
        text = text.replace(",", "").replace(" ", "")

        # Handle percentage
        is_percent = False
        if text.endswith("%"):
            text = text[:-1]
            is_percent = True

        # Handle parenthetical negatives: (123.45) -> -123.45
        if text.startswith("(") and text.endswith(")"):
            text = "-" + text[1:-1]

        # Handle leading negative signs and currency symbols
        for symbol in ("$", "₹", "€", "£", "¥"):
            text = text.replace(symbol, "")

        # Handle dash as zero
        if text in ("-", "—", "–", "‐"):
            return 0.0

        try:
            val = float(text)
            if is_percent:
                val = val / 100.0
            return val
        except ValueError:
            return None


@dataclass
class MergeRegion:
    """Describes a merged cell region in Excel coordinates (0-indexed)."""

    start_row: int
    start_col: int
    end_row: int
    end_col: int


@dataclass
class TableData:
    """
    Represents a fully reconstructed table ready for Excel output.
    """

    title: str = ""
    headers: list[list[str]] = field(default_factory=list)  # multi-level headers
    rows: list[list[str]] = field(default_factory=list)
    merge_regions: list[MergeRegion] = field(default_factory=list)
    page_number: int = 0
    confidence: float = 1.0  # 0.0 – 1.0, used for engine selection

    @property
    def total_rows(self) -> int:
        return len(self.headers) + len(self.rows)

    @property
    def total_cols(self) -> int:
        if self.headers:
            return max(len(row) for row in self.headers)
        if self.rows:
            return max(len(row) for row in self.rows)
        return 0

    @property
    def is_empty(self) -> bool:
        return self.total_rows == 0 or self.total_cols == 0


@dataclass
class ExtractionResult:
    """Complete result from processing a single PDF."""

    filename: str
    tables: list[TableData] = field(default_factory=list)
    page_count: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.tables) > 0 and len(self.errors) == 0

    @property
    def table_count(self) -> int:
        return len(self.tables)
