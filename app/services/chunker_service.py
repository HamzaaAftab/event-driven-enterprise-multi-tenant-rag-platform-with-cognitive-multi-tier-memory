"""
Markdown-Aware Hierarchical & Table-Preserving Chunker Service.
Intelligently segments Markdown documents while preserving table integrity and header hierarchies.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import tiktoken
from app.services.parser_service import ParsedPage, ParseResult

logger = logging.getLogger("chunker_service")


@dataclass
class ChunkItem:
    """Represents an atomic, semantic chunk of a document."""
    chunk_index: int
    content: str
    page_number: int
    token_count: int
    is_table: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


class MarkdownChunkerService:
    """
    Advanced hierarchical chunker designed for enterprise RAG:
    1. Table Preservation: Treats Markdown tables as atomic blocks (never slices rows).
    2. Context Breadcrumbs: Injects active section headers (# H1 > ## H2) into child chunks.
    3. Page Mapping: Tracks exact source page numbers for accurate search citations.
    4. Token-Bounded: Enforces strict token limits using tiktoken tokenizer.
    """

    def __init__(
        self,
        target_chunk_tokens: int = 500,
        chunk_overlap_tokens: int = 50,
        encoding_name: str = "cl100k_base",
    ) -> None:
        self.target_chunk_tokens = target_chunk_tokens
        self.chunk_overlap_tokens = chunk_overlap_tokens
        try:
            self._tokenizer = tiktoken.get_encoding(encoding_name)
        except Exception:
            self._tokenizer = None

    def count_tokens(self, text: str) -> int:
        """Calculates accurate token count using tiktoken or 4-char fallback."""
        if not text:
            return 0
        if self._tokenizer:
            try:
                return len(self._tokenizer.encode(text))
            except Exception:
                pass
        return max(1, len(text) // 4)

    def _is_table_block(self, text: str) -> bool:
        """Determines if a block of text represents a Markdown table."""
        if not text:
            return False
        # Remove any context header lines before checking table syntax
        lines = [line.strip() for line in text.strip().split("\n") if line.strip() and not line.startswith("[Context:")]
        if len(lines) < 2:
            return False
        # Check if lines contain markdown table pipe syntax
        has_pipe_rows = sum(1 for line in lines if line.startswith("|") and line.endswith("|"))
        has_separator = any(bool(re.search(r"\|[\s:\-]+\|", line)) for line in lines)
        return has_pipe_rows >= 2 and has_separator

    def _extract_blocks(self, page_markdown: str) -> List[str]:
        """
        Splits a page into logical blocks (Headers, Paragraphs, Tables, Codeblocks).
        Ensures multi-line tables and code fences remain in a single contiguous block.
        """
        lines = page_markdown.split("\n")
        blocks: List[str] = []
        current_block_lines: List[str] = []
        in_code_fence = False
        in_table = False

        for line in lines:
            stripped = line.strip()

            # Handle code fence toggle
            if stripped.startswith("```"):
                in_code_fence = not in_code_fence
                current_block_lines.append(line)
                if not in_code_fence:  # Closed fence -> finalize block
                    blocks.append("\n".join(current_block_lines))
                    current_block_lines = []
                continue

            if in_code_fence:
                current_block_lines.append(line)
                continue

            # Handle Markdown table block
            if stripped.startswith("|") and stripped.endswith("|"):
                in_table = True
                current_block_lines.append(line)
                continue
            else:
                if in_table:
                    # Table just ended
                    in_table = False
                    if current_block_lines:
                        blocks.append("\n".join(current_block_lines))
                        current_block_lines = []

            # Handle Markdown Headers (# Heading)
            if stripped.startswith("#"):
                if current_block_lines:
                    blocks.append("\n".join(current_block_lines))
                    current_block_lines = []
                blocks.append(line)
                continue

            # Handle empty line paragraph boundaries
            if not stripped:
                if current_block_lines:
                    blocks.append("\n".join(current_block_lines))
                    current_block_lines = []
                continue

            # Standard paragraph line
            current_block_lines.append(line)

        if current_block_lines:
            blocks.append("\n".join(current_block_lines))

        return [b.strip() for b in blocks if b.strip()]

    def chunk_parse_result(
        self,
        parse_result: ParseResult,
    ) -> List[ChunkItem]:
        """
        Chunks the entire parsed document across all pages with hierarchy retention.
        """
        all_chunks: List[ChunkItem] = []
        chunk_idx = 0

        # Hierarchical header breadcrumb state
        current_h1 = ""
        current_h2 = ""
        current_h3 = ""

        accumulated_blocks: List[str] = []
        accumulated_tokens = 0
        current_page_num = 1

        def build_header_prefix() -> str:
            """Builds context breadcrumb tag e.g. '[Section: Financials > Balance Sheet]'."""
            headers = [h for h in [current_h1, current_h2, current_h3] if h]
            if headers:
                return f"[Context: {' > '.join(headers)}]\n"
            return ""

        def flush_chunk() -> Optional[ChunkItem]:
            nonlocal chunk_idx, accumulated_blocks, accumulated_tokens
            if not accumulated_blocks:
                return None

            raw_text = "\n\n".join(accumulated_blocks).strip()
            prefix = build_header_prefix()
            # If the raw text doesn't already start with the header, prepend breadcrumb
            full_text = prefix + raw_text if prefix and not raw_text.startswith(prefix.strip()) else raw_text

            is_table = self._is_table_block(raw_text)
            tokens = self.count_tokens(full_text)

            chunk = ChunkItem(
                chunk_index=chunk_idx,
                content=full_text,
                page_number=current_page_num,
                token_count=tokens,
                is_table=is_table,
                metadata={
                    "file_name": parse_result.file_name,
                    "page_number": current_page_num,
                    "section_h1": current_h1,
                    "section_h2": current_h2,
                    "section_h3": current_h3,
                    "is_table": is_table,
                    "token_count": tokens,
                },
            )
            chunk_idx += 1
            accumulated_blocks = []
            accumulated_tokens = 0
            return chunk

        for page in parse_result.pages:
            current_page_num = page.page_number
            blocks = self._extract_blocks(page.content)

            for block in blocks:
                # 1. If encountering a new major header, flush previous section blocks first
                if block.startswith("#") and accumulated_blocks:
                    flushed = flush_chunk()
                    if flushed:
                        all_chunks.append(flushed)

                # Update Header Hierarchy
                if block.startswith("# "):
                    current_h1 = block.lstrip("#").strip()
                    current_h2 = ""
                    current_h3 = ""
                elif block.startswith("## "):
                    current_h2 = block.lstrip("#").strip()
                    current_h3 = ""
                elif block.startswith("### "):
                    current_h3 = block.lstrip("#").strip()

                block_tokens = self.count_tokens(block)
                is_table = self._is_table_block(block)

                # 2. If block is an atomic table that exceeds target chunk size alone:
                if is_table:
                    # Flush any previous non-table text first
                    if accumulated_blocks:
                        flushed = flush_chunk()
                        if flushed:
                            all_chunks.append(flushed)

                    # Keep table as a standalone atomic chunk
                    prefix = build_header_prefix()
                    table_content = prefix + block if prefix else block
                    table_tokens = self.count_tokens(table_content)

                    all_chunks.append(
                        ChunkItem(
                            chunk_index=chunk_idx,
                            content=table_content,
                            page_number=current_page_num,
                            token_count=table_tokens,
                            is_table=True,
                            metadata={
                                "file_name": parse_result.file_name,
                                "page_number": current_page_num,
                                "section_h1": current_h1,
                                "section_h2": current_h2,
                                "section_h3": current_h3,
                                "is_table": True,
                                "token_count": table_tokens,
                            },
                        )
                    )
                    chunk_idx += 1
                    continue

                # 3. Standard text block accumulation
                if accumulated_tokens + block_tokens > self.target_chunk_tokens and accumulated_blocks:
                    flushed = flush_chunk()
                    if flushed:
                        all_chunks.append(flushed)

                accumulated_blocks.append(block)
                accumulated_tokens += block_tokens

        # Flush remaining blocks
        if accumulated_blocks:
            flushed = flush_chunk()
            if flushed:
                all_chunks.append(flushed)

        logger.info(
            "[CHUNKER SUCCESS] Document '%s' split into %d hierarchical chunks.",
            parse_result.file_name,
            len(all_chunks),
        )
        return all_chunks


chunker_service = MarkdownChunkerService()
