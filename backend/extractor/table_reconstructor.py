"""
Table reconstruction: column splitting, row expansion, header detection.

Pipeline:
  1. Split merged columns (when pdfplumber merges space-separated sub-headers)
  2. Expand multi-line cells into separate rows
  3. Detect multi-level headers
  4. Build merge regions for Excel
"""

from __future__ import annotations

import logging
import re
from copy import deepcopy

from backend.models import MergeRegion, TableData

logger = logging.getLogger(__name__)

# Standard IRDAI financial report column keywords
_COL_KEYWORDS = frozenset({
    'LIFE', 'PENSION', 'HEALTH', 'ANNUITY', 'TOTAL',
    'VAR.INS', 'VAR.', 'INS', 'VARIABLE', 'INSURANCE',
})


class TableReconstructor:
    """Post-processes extracted tables for Excel output."""

    def reconstruct(self, tables: list[TableData]) -> list[TableData]:
        result: list[TableData] = []
        for table in tables:
            if table.is_empty:
                continue
            table = deepcopy(table)
            table = self._fix_corrupted_numbers(table)
            table = self._realign_misplaced_schedule_data(table)
            table = self._split_merged_columns(table)
            table = self._realign_displaced_columns(table)
            table = self._split_numeric_data_columns(table)
            table = self._expand_multiline_cells(table)
            table = self._detect_headers(table)
            table = self._strip_empty_columns(table)
            table = self._stitch_rows(table)
            table = self._deduplicate_rows(table)
            table = self._detect_merge_regions(table)
            result.append(table)
        return result

    def _stitch_rows(self, table: TableData) -> TableData:
        """
        Fix label-data displacement.
        """
        rows = table.rows
        if not rows:
            return table

        _section_keywords = {
            'APPROPRIATIONS', 'INCOME FROM INVESTMENTS', 'OTHER INCOME',
            'CHANGE IN VALUATION', 'PREMIUMS EARNED', 'NON-LINKED',
            'UNIT LINKED', 'PARTICIPATING', 'NON PARTICIPATING',
        }

        def _is_section_header(idx: int, lblock: list[list[str]]) -> bool:
            lbl = ' '.join(str(c).strip() for c in lblock[idx][0:2]).strip()
            if any(kw in lbl.upper() for kw in _section_keywords):
                return True
            if lbl.endswith(':'):
                return True
            # if this label is not a list marker, and next is a list marker
            is_marker = bool(re.match(r'^([a-z]\)|\d+\.|[ivx]+\))', lbl, re.IGNORECASE))
            if not is_marker:
                if idx + 1 < len(lblock):
                    nxt_lbl = ' '.join(str(c).strip() for c in lblock[idx+1][0:2]).strip()
                    if bool(re.match(r'^([a-z]\)|\d+\.|[ivx]+\))', nxt_lbl, re.IGNORECASE)):
                        return True
            return False

        new_rows = []
        i = 0

        while i < len(rows):
            current = rows[i]
            has_label = any(str(c).strip() for c in current[0:2])
            has_data = any(str(c).strip() for c in current[2:])
            is_label_only = has_label and not has_data
            
            if not is_label_only:
                new_rows.append(current)
                i += 1
                continue

            # Collect contiguous block of label-only rows
            label_block = [current]
            j = i + 1
            while j < len(rows):
                r = rows[j]
                hl = any(str(c).strip() for c in r[0:2])
                hd = any(str(c).strip() for c in r[2:])
                if hl and not hd:
                    label_block.append(r)
                    j += 1
                else:
                    break

            # Collect contiguous block of data-only rows that follow
            data_block = []
            while j < len(rows):
                r = rows[j]
                hl = any(str(c).strip() for c in r[0:2])
                hd = any(str(c).strip() for c in r[2:])
                if hd and not hl:
                    data_block.append(r)
                    j += 1
                else:
                    break

            if not data_block:
                new_rows.extend(label_block)
                i = j
                continue

            # Identify which labels are data-capable (not headers)
            data_capable_indices = [idx for idx in range(len(label_block)) 
                                    if not _is_section_header(idx, label_block)]
            
            n_capable = len(data_capable_indices)
            n_data = len(data_block)

            if n_data >= n_capable:
                # Pair 1:1, add extra data rows
                for k, dr in enumerate(data_block):
                    if k < n_capable:
                        lidx = data_capable_indices[k]
                        mlen = min(len(label_block[lidx]), len(dr))
                        for ci in range(2, mlen):
                            if str(dr[ci]).strip():
                                label_block[lidx][ci] = dr[ci]
                        if len(dr) > len(label_block[lidx]):
                            for ci in range(len(label_block[lidx]), len(dr)):
                                label_block[lidx].append(dr[ci] if str(dr[ci]).strip() else "")
                    else:
                        label_block.append(dr)
            else:
                # More capable labels than data — reverse-align
                unmatched_count = n_capable - n_data
                for k, dr in enumerate(data_block):
                    lidx = data_capable_indices[unmatched_count + k]
                    mlen = min(len(label_block[lidx]), len(dr))
                    for ci in range(2, mlen):
                        if str(dr[ci]).strip():
                            label_block[lidx][ci] = dr[ci]
                    if len(dr) > len(label_block[lidx]):
                        for ci in range(len(label_block[lidx]), len(dr)):
                            label_block[lidx].append(dr[ci] if str(dr[ci]).strip() else "")

            new_rows.extend(label_block)
            i = j

        table.rows = new_rows
        return table

    @staticmethod
    def _deduplicate_rows(table: TableData) -> TableData:
        """
        Remove duplicate data rows that arise when pdfplumber extracts a massive
        text blob in one cell AND also extracts the same data in proper columns.
        We keep the properly-structured version (more non-empty columns).
        
        Two dedup strategies:
        1. Numeric signature match: rows with identical sorted numeric values
        2. Full content match: rows with identical label + data values
        """
        if not table.rows or len(table.rows) < 5:
            return table

        def _row_signature(row: list[str]) -> str:
            """Create a signature from the numeric values in the row for comparison."""
            nums = []
            for c in row:
                s = str(c).strip().replace(',', '').replace(' ', '')
                if s.startswith('(') and s.endswith(')'):
                    s = s[1:-1]
                if s.replace('.', '').replace('-', '').isdigit() and len(s) >= 2:
                    nums.append(s)
            return '|'.join(sorted(nums[:5]))

        def _full_content_key(row: list[str]) -> str:
            """Create a content key from label + all cell values."""
            label = str(row[0]).strip() if row else ""
            vals = tuple(str(c).strip() for c in row[1:])
            return f"{label}||{'|'.join(vals)}"

        # Strategy 1: Numeric signature dedup
        row_sigs = [(i, _row_signature(table.rows[i])) for i in range(len(table.rows))]
        sig_map: dict[str, list[int]] = {}
        for idx, sig in row_sigs:
            if sig and len(sig) > 5:
                sig_map.setdefault(sig, []).append(idx)

        remove_indices = set()
        for sig, indices in sig_map.items():
            if len(indices) <= 1:
                continue
            best_idx = max(indices, key=lambda i: sum(1 for c in table.rows[i] if str(c).strip()))
            for idx in indices:
                if idx != best_idx:
                    remove_indices.add(idx)

        # Strategy 2: Full content dedup (catches dash-heavy rows)
        content_map: dict[str, list[int]] = {}
        for i, row in enumerate(table.rows):
            if i in remove_indices:
                continue
            key = _full_content_key(row)
            if key and len(key) > 5:
                content_map.setdefault(key, []).append(i)

        for key, indices in content_map.items():
            if len(indices) <= 1:
                continue
            # Keep the first occurrence, remove subsequent duplicates
            for idx in indices[1:]:
                remove_indices.add(idx)

        if remove_indices:
            logger.info("Deduplicating %d duplicate rows", len(remove_indices))
            table.rows = [r for i, r in enumerate(table.rows) if i not in remove_indices]

        return table

    # ══════════════════════════════════════════════════════════════════════
    #  STEP 1: Realign misaligned schedule columns
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _realign_misplaced_schedule_data(table: TableData) -> TableData:
        """
        Fixes an extraction glitch where a row that lacks a Schedule code 
        accidentally drops its massive numeric data block into the Schedule column (C1).
        This identifies the Schedule column, and shifts large numeric anomalies to the right.
        """
        all_rows = table.headers + table.rows
        if not all_rows:
            return table

        num_cols = len(all_rows[0])
        schedule_cols = []
        for row in all_rows[:10]:
            for ci in range(min(num_cols, len(row))):
                if 'SCHEDULE' in str(row[ci]).upper().strip():
                    if ci not in schedule_cols:
                        schedule_cols.append(ci)
        
        if not schedule_cols:
            return table

        def is_huge_numeric(text: str) -> bool:
            if not text: return False
            clean = re.sub(r'[\s,\.\-\(\)]', '', text)
            if clean and sum(1 for c in clean if c.isdigit()) / len(clean) > 0.5:
                # Must have multiple space-separated tokens to be "huge" data row
                return len(text.split()) >= 3
            return False

        for ci in schedule_cols:
            if ci + 1 >= num_cols: continue
            for row in all_rows:
                while len(row) <= ci + 1:
                    row.append("")
                    
                val = row[ci]
                next_val = row[ci+1]
                
                # If Schedule cell has a huge block of numbers AND the next cell is empty
                if is_huge_numeric(val) and not next_val.strip():
                    logger.info("Realigning mis-shifted numeric data from Schedule col %d", ci)
                    row[ci+1] = val
                    row[ci] = ""
                    
        return table

    @staticmethod
    def _split_numeric_data_columns(table: TableData) -> TableData:
        """
        Detects if a column is consistently space-delimited with N tokens, 
        and splits it if it's purely numeric data.
        """
        all_rows = table.headers + table.rows
        if not all_rows:
            return table
            
        num_cols = len(all_rows[0])
        col_splits = {}
        
        for ci in range(num_cols):
            tokens_count = {}
            data_cells_count = 0
            for row in all_rows:
                cell = row[ci] if ci < len(row) else ""
                lines = cell.split('\n')
                for line in lines:
                    line = line.strip()
                    if not line: continue
                    # Fix spaces inside parens explicitly for counting
                    line = re.sub(r'\(\s+', '(', line)
                    line = re.sub(r'\s+\)', ')', line)
                    
                    if re.match(r'^[-,\.\(\)\d\s]+$', line) and any(char.isdigit() for char in line):
                        tokens = line.split()
                        if len(tokens) > 1:
                            tokens_count[len(tokens)] = tokens_count.get(len(tokens), 0) + 1
                        data_cells_count += 1
            
            if data_cells_count >= 3 and tokens_count:
                best_k = max(tokens_count.keys(), key=lambda k: tokens_count[k])
                # Only split if at least 30% of numeric cells have exactly best_k tokens
                if best_k > 1 and tokens_count[best_k] >= data_cells_count * 0.3:
                    col_splits[ci] = best_k
        
        if not col_splits:
            return table
            
        logger.info("Splitting numeric data columns: %s", col_splits)
        
        new_all: list[list[str]] = []
        for ri, row in enumerate(all_rows):
            new_row: list[str] = []
            for ci in range(num_cols):
                cell = row[ci] if ci < len(row) else ""
                if ci in col_splits:
                    n = col_splits[ci]
                    # We need to split this cell by spaces, preserving newlines
                    lines = cell.split('\n')
                    split_lines = [ [] for _ in range(n) ]
                    for line in lines:
                        line = line.strip()
                        line = re.sub(r'\(\s+', '(', line)
                        line = re.sub(r'\s+\)', ')', line)
                        
                        if not line:
                            for c_idx in range(n): split_lines[c_idx].append("")
                        else:
                            # Mostly numeric data?
                            is_numeric_data = bool(re.match(r'^[-,\.\(\)\d\s]+$', line)) and (
                                any(char.isdigit() for char in line) or re.match(r'^[\s\-]+$', line)
                            )
                            if is_numeric_data:
                                tokens = line.split()
                                if len(tokens) == n:
                                    for c_idx in range(n): split_lines[c_idx].append(tokens[c_idx])
                                elif len(tokens) < n:
                                    # Pad with empty
                                    for c_idx in range(len(tokens)): split_lines[c_idx].append(tokens[c_idx])
                                    for c_idx in range(len(tokens), n): split_lines[c_idx].append("")
                                else:
                                    # Too many tokens? Just put the first n-1, and join the rest
                                    for c_idx in range(n-1): split_lines[c_idx].append(tokens[c_idx])
                                    split_lines[n-1].append(" ".join(tokens[n-1:]))
                            else:
                                # Header or non-numeric line — keep it in the first split column
                                split_lines[0].append(line)
                                for c_idx in range(1, n): split_lines[c_idx].append("")
                    
                    # Convert split lines back to multiline strings
                    for c_idx in range(n):
                        new_row.append("\n".join(split_lines[c_idx]).strip())
                else:
                    new_row.append(cell)
            new_all.append(new_row)
            
        n_headers = len(table.headers)
        table.headers = new_all[:n_headers]
        table.rows = new_all[n_headers:]
        return table

    @staticmethod
    def _split_merged_columns(table: TableData) -> TableData:
        """
        Detect cells containing multiple space-separated sub-header names
        (e.g. 'LIFE PENSION HEALTH VAR.INS') and split them into real columns.
        
        Only activates when at least one cell contains 3+ column keywords,
        indicating that pdfplumber merged multiple sub-columns into one cell
        because of missing vertical lines/rects.
        
        GUARD: Only activates when a *single cell* has 3+ column keywords that
        make up the majority of the cell text (>70% keyword words). This
        prevents false positives like company names ("SBI LIFE INSURANCE").
        """
        all_rows = table.headers + table.rows
        if not all_rows:
            return table

        # Find sub-header row: look for cells that are MOSTLY column keywords
        sub_idx = -1
        max_kw = 0
        for ri, row in enumerate(all_rows[:10]):
            for cell in row:
                if not cell:
                    continue
                text = cell.upper()
                # Normalize multi-word column names
                text = re.sub(r'VAR\.\s*INS\b', 'VAR.INS', text)
                text = re.sub(r'VARIABLE\s+INSURANCE', 'VAR.INS', text)
                words = text.split()
                if not words:
                    continue
                kw_count = sum(1 for w in words if w in _COL_KEYWORDS)
                kw_ratio = kw_count / len(words)
                # Must have 3+ keywords AND they must be majority (>70%) of words
                if kw_count >= 3 and kw_ratio > 0.7 and kw_count > max_kw:
                    max_kw = kw_count
                    sub_idx = ri

        if sub_idx < 0:
            return table  # No merged columns detected

        sub_row = all_rows[sub_idx]
        
        # Build split plan for each column
        col_plan = []  # list of (n_subcols, sub_names)
        needs_split = False
        
        for cell in sub_row:
            sub_names = _parse_sub_header(cell)
            col_plan.append((len(sub_names), sub_names))
            if len(sub_names) > 1:
                needs_split = True

        if not needs_split:
            return table

        logger.info("Splitting merged columns: %s → %s cols",
                     len(sub_row), sum(n for n, _ in col_plan))

        # Split every row
        new_all: list[list[str]] = []
        for ri, row in enumerate(all_rows):
            new_row: list[str] = []
            for ci, (n, sub_names) in enumerate(col_plan):
                cell = row[ci] if ci < len(row) else ""
                if n <= 1:
                    new_row.append(cell)
                elif ri == sub_idx:
                    # Sub-header row — use parsed names
                    new_row.extend(sub_names)
                elif ri < sub_idx:
                    # Parent header rows (LINKED BUSINESS, PARTICIPATING etc.)
                    # Place text in first sub-column, empty for rest → merge span
                    new_row.append(cell)
                    new_row.extend([""] * (n - 1))
                else:
                    # Data rows — split values
                    new_row.extend(_split_data_cell(cell, n))
            new_all.append(new_row)

        # Reconstruct headers + rows
        n_headers = len(table.headers)
        table.headers = new_all[:n_headers]
        table.rows = new_all[n_headers:]
        return table

    # ══════════════════════════════════════════════════════════════════════
    #  STEP 2: Expand multi-line cells into rows
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _fix_corrupted_numbers(table: TableData) -> TableData:
        """
        Fix space-corrupted numbers from pdfplumber.
        """
        all_rows = table.headers + table.rows
        for row in all_rows:
            for ci in range(len(row)):
                cell = row[ci]
                if not cell:
                    continue
                # Split by lines to handle multiline cells without breaking newlines
                lines = cell.split('\n')
                fixed_lines = []
                for line in lines:
                    # Drop garbage lines containing #REF! (artifact from PDF creation Excel errors)
                    if 'REF!' in line:
                        clean_ref = re.sub(r'[\s\-#REF!]', '', line)
                        if not clean_ref:
                            fixed_lines.append("")
                            continue
                            
                    fixed = line
                    # 1. Fix spaces inside parentheses: "( 207 )" -> "(207)"
                    fixed = re.sub(r'\(\s+', '(', fixed)
                    fixed = re.sub(r'\s+\)', ')', fixed)
                    
                    # 2. Fix space before commas: "4 ,579" or "1 ,10,698"
                    fixed = re.sub(r'(\d)\s+(,\d{2,3}(?:,\d{2,3})*)', r'\1\2', fixed)
                    
                    # 3. Fix double-digit separated with comma downstream: "4 6,046"
                    fixed = re.sub(r'(^|\s|\()(\d{1,2})\s+(\d{1,2},\d+)', r'\1\2\3', fixed)
                    
                    # 4. Fix separated digits without commas if they look like a single number 
                    #    "3 02" -> "302" but only if strictly 2-3 digits follow
                    fixed = re.sub(r'(^|\s|\()(\d{1,2})\s+(\d{2,3})(?=\s|$|\))', r'\1\2\3', fixed)
                    
                    fixed_lines.append(fixed)
                
                row[ci] = '\n'.join(fixed_lines)
        return table

    @staticmethod
    def _realign_displaced_columns(table: TableData) -> TableData:
        """
        Fixes the issue where pdfplumber bundles an entire column's text into a 
        single massive multiline string in one cell, leaving the subsequent rows 
        completely empty in that column. This redistributes the overflow down into 
        those empty cells, constrained by the line counts of other columns.
        """
        all_rows = table.headers + table.rows
        if not all_rows:
            return table
            
        num_cols = len(all_rows[0])
        
        for ci in range(num_cols):
            pending_lines = []
            for ri, row in enumerate(all_rows):
                cell = row[ci] if ci < len(row) else ""
                
                # Calculate how many lines this row *structurally* requires
                # by checking the maximum lines among other populated columns
                other_lines = []
                for k in range(len(row)):
                    if k != ci and row[k].strip():
                        other_lines.append(len(row[k].split('\n')))
                req_lines = max(other_lines) if other_lines else 1
                
                if cell.strip():
                    lines = cell.split('\n')
                    if len(lines) > req_lines and ri + 1 < len(all_rows):
                        # The cell has more lines than the row requires.
                        # Check if the very next cell in this column is empty,
                        # which indicates column displacement.
                        next_cell = all_rows[ri+1][ci] if ci < len(all_rows[ri+1]) else ""
                        if not next_cell.strip():
                            row[ci] = "\n".join(lines[:req_lines])
                            pending_lines = lines[req_lines:]
                elif not cell.strip() and pending_lines:
                    # Feed from the pending overflow buffer
                    take = pending_lines[:req_lines]
                    row[ci] = "\n".join(take)
                    pending_lines = pending_lines[req_lines:]
                    
        return table

    @staticmethod
    def _expand_multiline_cells(table: TableData) -> TableData:
        """
        Split cells containing '\\n' into separate rows with smarter logic for lists and data.
        
        RULES:
        1. 2+ multi-line columns → expand (each sub-line = new row)
        2. Single multiline column → expand if it looks like a numeric/serial data list
        3. Only 1–2 non-empty cells (merged title) → collapse UNLESS it looks like a data list (e.g. serials)
        4. C0 Label List Capping: if C0 has >>more lines than data columns, cap expansion
        """
        all_rows = table.headers + table.rows
        if not all_rows:
            return table

        expanded: list[list[str]] = []

        for row in all_rows:
            cell_texts = []
            for cell in row:
                text = cell if cell and cell != "None" else ""
                # Do NOT strip() the entire text, because that removes leading \n 
                # which are critically needed to align data with the correct label row.
                cell_texts.append(text)

            non_empty = [c for c in cell_texts if c]
            multiline_indices = [i for i, c in enumerate(cell_texts) if "\n" in c]
            
            # Identify if ANY cell in this row looks like a vertical data list (serials or financial values)
            is_data_list = any(TableReconstructor._looks_like_data_list(cell_texts[i]) for i in multiline_indices)
            
            should_expand = False
            if len(multiline_indices) >= 2:
                should_expand = True
            elif len(multiline_indices) == 1 and is_data_list:
                should_expand = True
            elif is_data_list:
                should_expand = True
                
            # If a single cell has a massive amount of lines, we need to know if it's a lumped data 
            # column (which needs expansion) or just a giant top-level header title block (which shouldn't be expanded).
            has_massive_cell = any(c.count('\n') >= 3 for c in cell_texts)
            is_header_title_block = len(non_empty) == 1 and non_empty[0].count('\n') >= 4 and len(expanded) < 3

            if len(non_empty) <= 2 and not is_data_list and (not has_massive_cell or is_header_title_block):
                # True title/merged row (not a data list), or a giant header block — collapse
                collapsed = [c.replace("\n", " ").strip() for c in cell_texts]
                expanded.append(collapsed)
            elif should_expand or (has_massive_cell and not is_header_title_block):
                # Expand sub-rows
                split_cells = []
                max_lines = 1
                max_data_lines = 1  # Max lines in columns OTHER than C0
                for ci, text in enumerate(cell_texts):
                    lines = [l.strip() for l in text.split("\n")] if text and "\n" in text else [text]
                    split_cells.append(lines)
                    max_lines = max(max_lines, len(lines))
                    if ci > 0:
                        max_data_lines = max(max_data_lines, len(lines))

                # Smart cap for C0 Label Lists (e.g. adityabirla case)
                # Only cap if there is actually data in other columns to align with
                c0_lines = len(split_cells[0]) if split_cells else 1
                if c0_lines > max_data_lines * 3 and max_data_lines > 1:
                    expand_count = max_data_lines
                    # Join remaining C0 labels into last expanded row
                    for li in range(expand_count):
                        new_row = []
                        for ci, cl in enumerate(split_cells):
                            if ci == 0 and li == expand_count - 1:
                                # Last row: join remaining C0 lines
                                remaining = [l for l in cl[li:] if l.strip()]
                                new_row.append("\n".join(remaining))
                            elif li < len(cl):
                                new_row.append(cl[li])
                            else:
                                new_row.append("")
                        expanded.append(new_row)
                else:
                    for li in range(max_lines):
                        new_row = []
                        for cl in split_cells:
                            if li < len(cl):
                                new_row.append(cl[li])
                            elif len(cl) == 1 and li > 0:
                                # Do NOT duplicate a single-line label across all expanded rows
                                new_row.append("")
                            else:
                                new_row.append("")
                        expanded.append(new_row)
            else:
                # No expansion needed — collapse newlines to avoid row height issues
                collapsed = [c.replace("\n", " ").strip() for c in cell_texts]
                expanded.append(collapsed)

        expanded = [r for r in expanded if any(c.strip() for c in r)]

        # Merge continuation lines: If a row is label-only and the label
        # starts with a lowercase letter or is a single short word (like "Funds"),
        # it's likely a text-wrap continuation of the previous row's label.
        if len(expanded) > 1:
            merged = [expanded[0]]
            for row in expanded[1:]:
                # Find the main label column (first non-empty text column)
                label_col = -1
                label_text = ""
                for ci, c in enumerate(row):
                    if c.strip() and not TableReconstructor._is_numeric(c.strip()):
                        label_col = ci
                        label_text = c.strip()
                        break
                
                # Check if this row has data in columns beyond the label columns
                has_data = False
                if label_col >= 0:
                    for ci in range(max(label_col + 1, 2), len(row)):
                        if ci < len(row) and row[ci].strip():
                            has_data = True
                            break
                
                if (not has_data and label_text and label_col >= 0 and merged
                    and not TableReconstructor._is_numeric(label_text)):
                    # Check if it looks like a continuation:
                    # - Starts with lowercase letter
                    # - Is a single short word (< 20 chars) without a prefix like (a), 1., etc.
                    # - Doesn't start with a standard item prefix
                    is_item_prefix = bool(re.match(
                        r'^(\d+[\.\)\s]|[a-z]\)|[a-z]{1,2}\)|'
                        r'\([a-z]\)|\([a-z]{2}\)|'
                        r'SUB|TOTAL|SURPLUS|DEFICIT|AMOUNT|BENEFIT|'
                        r'Commission|Operating|Provision|Bad|Goods|'
                        r'Investments? |Other |Transfer|Balance|APPROPRIATION)',
                        label_text, re.IGNORECASE
                    ))
                    
                    if is_item_prefix:
                        is_continuation = False
                    else:
                        is_continuation = (
                            (label_text[0].islower() and len(label_text) < 100)
                            or label_text.startswith('(')
                            or (len(label_text.split()) <= 4 and len(label_text) < 40)
                        )
                    
                    if is_continuation:
                        # Merge into previous row's label
                        prev = merged[-1]
                        if label_col < len(prev):
                            prev_text = prev[label_col].strip()
                            prev[label_col] = prev_text + " " + label_text if prev_text else label_text
                        continue
                
                merged.append(row)
            expanded = merged

        if not expanded:
            table.headers = []
            table.rows = []
            return table

        table.headers = [expanded[0]]
        table.rows = expanded[1:]
        return table

    # ══════════════════════════════════════════════════════════════════════
    #  STEP 3: Header detection
    # ══════════════════════════════════════════════════════════════════════

    def _detect_headers(self, table: TableData) -> TableData:
        all_rows = table.headers + table.rows
        if not all_rows:
            return table

        header_rows: list[list[str]] = []
        data_rows: list[list[str]] = []
        found_data = False

        for ri, row in enumerate(all_rows):
            if not found_data and self._is_header_row(row, ri):
                header_rows.append(row)
            else:
                found_data = True
                data_rows.append(row)

        if not header_rows and data_rows:
            header_rows = [data_rows.pop(0)]
        # Allow up to 8 header rows for complex multi-level financial tables
        if len(header_rows) > 8:
            data_rows = header_rows[8:] + data_rows
            header_rows = header_rows[:8]

        table.headers = header_rows
        table.rows = data_rows
        return table

    def _is_header_row(self, row: list[str], row_idx: int) -> bool:
        non_empty = [c for c in row if c.strip()]
        if not non_empty:
            return True  # Empty rows in header section are kept
        numeric = sum(1 for c in non_empty if self._is_numeric(c))
        text = len(non_empty) - numeric
        # Single cell: header if it's text (not a numbered item like "1 Available...")
        if len(non_empty) == 1:
            val = non_empty[0].strip()
            # Item numbers followed by text are data, not headers
            if re.match(r'^\d+[\s\.\)]', val) or re.match(r'^[a-z]\)', val.lower()):
                return False
            
            # Very first row is almost always a header title
            if row_idx == 0:
                return True
                
            # Standard structural header keywords
            keywords = ["PARTICULARS", "SCHEDULE", "AS AT", "TOTAL", "QUARTER", "YEAR"]
            if any(kw in val.upper() for kw in keywords):
                return True
                
            # If it's not fully uppercase and appears in the first column, 
            # it's very likely a data categorisation label (e.g. "Secured", "In India").
            # However, if it's very early in the table (row 1 or 2), it might be 
            # a subtitle like "Name of the Insurer: ...", which IS a header.
            if row[0].strip() == val and not val.isupper() and row_idx >= 3:
                return False
                
            return text == 1
        return text > numeric

    # ══════════════════════════════════════════════════════════════════════
    #  STEP 3b: Strip empty leading/trailing columns
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _strip_empty_columns(table: TableData) -> TableData:
        """
        Remove leading/trailing columns that are mostly empty (>80%) in
        data rows, handling pdfplumber merged title cells.
        
        For SBI-style tables: C0/C1 contain full title text in row 0-1 and
        entire-row text blobs in rows 34+ (spillover). The real data starts
        from C2. This method:
        1. Detects title columns (>80% empty in data rows)
        2. For data rows where ONLY title columns have content (C2+ empty),
           parses the text blob and redistributes values across columns
        3. Strips the title columns
        """
        all_rows = table.headers + table.rows
        if not all_rows or not table.rows:
            return table

        ncols = max(len(r) for r in all_rows)
        if ncols <= 2:
            return table

        # Count empty rate per column in data rows
        col_empty_rate = {}
        for ci in range(ncols):
            empty = sum(1 for row in table.rows
                        if ci >= len(row) or not row[ci].strip())
            col_empty_rate[ci] = empty / len(table.rows)

        # Find leading columns that are >60% empty IN DATA ROWS
        # BUT also check headers — don't strip columns with header content
        # (sbi: ghost cols have empty headers. adityabirla: C0='Particulars')
        def _has_header_content(ci: int) -> bool:
            """Check if a STRUCTURAL header row has content in column ci.
            Title/spanning rows (only 1-2 non-empty cells) don't count.
            """
            for hrow in table.headers:
                non_empty_count = sum(1 for c in hrow if c.strip())
                if non_empty_count < 3:
                    continue  # Title/spanning row — skip
                if ci < len(hrow) and hrow[ci].strip():
                    return True
            return False

        first_keep = 0
        while first_keep < ncols:
            if col_empty_rate.get(first_keep, 1) <= 0.60:
                break  # Not empty enough to strip
            if _has_header_content(first_keep):
                break  # Has header content — don't strip
            first_keep += 1

        # Find trailing columns that are >60% empty (also check headers)
        last_keep = ncols - 1
        while last_keep >= 0:
            if col_empty_rate.get(last_keep, 1) <= 0.60:
                break
            if _has_header_content(last_keep):
                break
            last_keep -= 1

        if first_keep == 0 and last_keep == ncols - 1:
            return table  # nothing to strip

        keep_cols = list(range(first_keep, last_keep + 1))
        if not keep_cols or len(keep_cols) == ncols:
            return table

        n_keep = len(keep_cols)
        strip_leading = first_keep  # number of leading cols to strip

        logger.info("Stripping %d leading, %d trailing columns (of %d)",
                     strip_leading, ncols - 1 - last_keep, ncols)

        # Process data rows: redistribute text blobs from stripped columns
        new_rows = []
        for row in table.rows:
            # Check if ALL kept columns are empty but stripped columns have data
            stripped_data = ""
            for ci in range(strip_leading):
                val = row[ci].strip() if ci < len(row) else ""
                if val and len(val) > len(stripped_data):
                    stripped_data = val

            kept_vals = [row[ci].strip() if ci < len(row) else ""
                         for ci in keep_cols]
            kept_has_data = any(v for v in kept_vals)

            if stripped_data and not kept_has_data:
                # Text blob in stripped cols, nothing in kept cols
                # Parse and redistribute
                new_row = _redistribute_text_blob(stripped_data, n_keep)
            else:
                # Normal row — just keep the right columns
                new_row = [row[ci] if ci < len(row) else "" for ci in keep_cols]

            new_rows.append(new_row)

        # Process header rows — just keep the right columns
        new_headers = []
        for row in table.headers:
            new_headers.append([row[ci] if ci < len(row) else ""
                                for ci in keep_cols])

        table.headers = new_headers
        table.rows = new_rows
        return table

    # ══════════════════════════════════════════════════════════════════════
    #  STEP 4: Merge region detection
    # ══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _detect_merge_regions(table: TableData) -> TableData:
        merges: list[MergeRegion] = []

        for row_idx, row in enumerate(table.headers):
            col = 0
            while col < len(row):
                cell = row[col].strip()
                if cell:
                    span = 1
                    while (col + span) < len(row) and not row[col + span].strip():
                        span += 1
                    if span > 1:
                        merges.append(MergeRegion(
                            start_row=row_idx, start_col=col,
                            end_row=row_idx, end_col=col + span - 1,
                        ))
                    col += span
                else:
                    col += 1

        if len(table.headers) > 1:
            ncols = table.total_cols
            for col_idx in range(ncols):
                first_row = None
                for row_idx in range(len(table.headers)):
                    val = ""
                    if col_idx < len(table.headers[row_idx]):
                        val = table.headers[row_idx][col_idx].strip()
                    if val and first_row is None:
                        first_row = row_idx
                    elif val and first_row is not None:
                        if row_idx - first_row > 1:
                            all_empty = all(
                                not (table.headers[r][col_idx].strip()
                                     if col_idx < len(table.headers[r]) else True)
                                for r in range(first_row + 1, row_idx)
                            )
                            if all_empty:
                                merges.append(MergeRegion(
                                    start_row=first_row, start_col=col_idx,
                                    end_row=row_idx - 1, end_col=col_idx,
                                ))
                        first_row = row_idx
                if first_row is not None and first_row < len(table.headers) - 1:
                    all_empty = all(
                        not (table.headers[r][col_idx].strip()
                             if col_idx < len(table.headers[r]) else True)
                        for r in range(first_row + 1, len(table.headers))
                    )
                    if all_empty:
                        merges.append(MergeRegion(
                            start_row=first_row, start_col=col_idx,
                            end_row=len(table.headers) - 1, end_col=col_idx,
                        ))

        table.merge_regions = merges
        return table

    @staticmethod
    def _is_numeric(text: str) -> bool:
        if not text or not text.strip():
            return False
        s = text.strip().replace(",", "").replace(" ", "")
        for sym in ("$", "₹", "€", "£", "¥"):
            s = s.replace(sym, "")
        if s.startswith("(") and s.endswith(")"):
            s = s[1:-1]
        if s.endswith("%"):
            s = s[:-1]
        if s in ("-", "—", "–", "‐"):
            return True
        try:
            float(s)
            return True
        except ValueError:
            return False

    @staticmethod
    def _looks_like_data_list(text: str) -> bool:
        """
        Check if multiline text looks like a vertical list of data items.
        Common cases:
          1. Serial numbers: '1\n2\n3'
          2. Financial values: '1,234.00\n5,678.00\n-'
        """
        if "\n" not in text:
            return False
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        if not lines:
            return False
            
        # Case 1: Numeric serial numbers (1, 2, 3...)
        is_serial = all(re.match(r'^\d+[\.\)]?$', l) for l in lines)
        if is_serial:
            return True
            
        # Case 2: Vertical list of financial numbers
        is_financial = all(TableReconstructor._is_numeric(l) or l in ("-", "NIL", "Nil", ".") for l in lines)
        if is_financial and len(lines) >= 2:
            return True
            
        return False


# ══════════════════════════════════════════════════════════════════════
#  Module-level helper functions for column splitting
# ══════════════════════════════════════════════════════════════════════

def _parse_sub_header(cell_text: str) -> list[str]:
    """Parse sub-header names from a merged header cell."""
    text = cell_text.strip()
    if not text:
        return [text]

    # Pre-process: combine known multi-word column names
    text = re.sub(r'VAR\.\s*INS\b', 'VAR.INS', text, flags=re.IGNORECASE)
    text = re.sub(r'VARIABLE\s+INSURANCE', 'VARIABLE_INSURANCE', text, flags=re.IGNORECASE)

    words = text.split()
    if len(words) <= 1:
        return [text]

    # Check if this cell actually SHOULD be split (has column keywords)
    upper_words = [w.upper() for w in words]
    kw_count = sum(1 for w in upper_words if w in _COL_KEYWORDS)
    if kw_count < 2:
        # Not enough column keywords — don't split
        return [text]

    # Restore multi-word names
    return [w.replace('VARIABLE_INSURANCE', 'VARIABLE INSURANCE') for w in words]


def _split_data_cell(cell_text: str, expected_count: int) -> list[str]:
    """
    Split a data cell into expected_count financial values.
    
    For multi-line cells (containing \\n), split EACH line independently
    and reconstruct per-column multi-line values so the row expander
    can properly expand them later.
    
    Example:
      Input: "14,61,613 1,03,974 - -\\n(6,728) - - -\\n-"
      Expected: 4
      Output: ["14,61,613\\n(6,728)\\n-", "1,03,974\\n-\\n", "-\\n-\\n", "-\\n-\\n"]
    """
    text = cell_text.strip()
    if not text:
        return [""] * expected_count

    if "\n" not in text:
        return _split_values(text, expected_count)

    # Multi-line: split each line independently
    lines = text.split("\n")
    # Split each line into expected_count values
    split_lines = [_split_values(line.strip(), expected_count) for line in lines]

    # Reconstruct: for each sub-column, join all lines with \n
    result = []
    for col_idx in range(expected_count):
        col_values = [sl[col_idx] if col_idx < len(sl) else "" for sl in split_lines]
        result.append("\n".join(col_values))

    return result


def _split_values(text: str, expected: int) -> list[str]:
    """Split text into financial values using regex."""
    if not text.strip():
        return [""] * expected

    # Match: parenthesized numbers, comma numbers, plain numbers, dashes
    tokens = re.findall(r'\([0-9,. ]+\)|[0-9][0-9,.]*|-(?!\w)', text)
    tokens = [t.replace(' ', '') for t in tokens]

    if len(tokens) == expected:
        return tokens

    if len(tokens) > expected:
        # Try merging split numbers: "1" + ",03,974" → "1,03,974"
        merged = _merge_split_numbers(tokens)
        if len(merged) == expected:
            return merged
        return merged[:expected] + [""] * max(0, expected - len(merged))

    # Fewer tokens — pad
    return tokens + [""] * (expected - len(tokens))


def _merge_split_numbers(tokens: list[str]) -> list[str]:
    """Merge tokens that were split mid-number (e.g. '1' + ',03,974')."""
    merged = []
    i = 0
    while i < len(tokens):
        if (i + 1 < len(tokens)
                and len(tokens[i]) <= 2
                and tokens[i].replace(',', '').isdigit()
                and tokens[i + 1][0:1] in (',', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
                and ',' in tokens[i + 1]):
            merged.append(tokens[i] + tokens[i + 1])
            i += 2
        else:
            merged.append(tokens[i])
            i += 1
    return merged


def _redistribute_text_blob(text: str, ncols: int) -> list[str]:
    """
    Parse a text blob that contains an entire row's data in a single string.
    
    Example: "Total (C) 22,84,865 5,21,181 28,06,045 2,78,602 8,468 356"
    Target: ncols=16 → ["Total (C)", "", "22,84,865", "5,21,181", ...]
    
    Also handles: "Transfer to other reserves - - - - - - - - - - - - - -"
    Target: ncols=16 → ["Transfer to other reserves", "", "-", "-", ...]
    
    Strategy:
    1. Find the boundary between label text and data values
    2. Extract all financial numbers and standalone dashes
    3. Place label in Col 0, skip Col 1 (Schedule), values in Col 2+
    """
    text = text.strip()
    if not text:
        return [""] * ncols

    # Token pattern: parenthesized numbers, comma numbers, standalone dashes
    _tok_pattern = re.compile(
        r'\(\s*[0-9][0-9,. ]*\s*\)'  # parenthesized: (1,234) or ( 0)
        r'|[0-9][0-9,.]*'             # plain number: 1,234 or 42849
        r'|-(?=\s|$)'                  # standalone dash (zero placeholder)
    )

    # Find ALL tokens in the string
    all_matches = list(_tok_pattern.finditer(text))
    
    if not all_matches:
        # No numeric tokens — just a label row
        return [text] + [""] * (ncols - 1)

    # Find the split point: where the label ends and values begin.
    # Strategy: look for the first match where the remaining text is
    # entirely composed of numbers/dashes/spaces (i.e., a data sequence).
    # This handles "Total (C) 22,84,865 ..." where "(C)" must NOT be
    # mistaken for a number token.
    split_idx = len(text)
    for mi, m in enumerate(all_matches):
        candidate_start = m.start()
        remaining = text[candidate_start:]
        # Check if the remaining string is ONLY numbers, dashes, parens, commas, spaces
        cleaned_remaining = re.sub(
            r'\(\s*[0-9][0-9,. ]*\s*\)|[0-9][0-9,.]*|\s+|-', '', remaining
        ).strip()
        if not cleaned_remaining:
            split_idx = candidate_start
            break

    label = text[:split_idx].strip()
    num_text = text[split_idx:]

    # Re-extract tokens from the clean numeric portion
    tokens = _tok_pattern.findall(num_text)
    tokens = [t.replace(' ', '') for t in tokens]
    
    # Merge split numbers (e.g., "22" + ",84,865")
    tokens = _merge_split_numbers(tokens)
    
    # Build result: label in Col 0, empty Col 1 (Schedule), values from Col 2+
    result = [""] * ncols
    result[0] = label
    
    # Place values starting from Col 2
    for i, val in enumerate(tokens):
        col_idx = 2 + i  # Skip Col 0 (label) and Col 1 (schedule ref)
        if col_idx < ncols:
            result[col_idx] = val
    
    return result
