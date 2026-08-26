"""
Document Parser Service - Advanced Layout & Table Extraction via LlamaParse / LlamaCloud.
Converts complex binary PDFs into clean, table-preserving Markdown with page mapping.
"""

import logging
import os
import re
import tempfile
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from llama_parse import LlamaParse
from app.core.config import settings

logger = logging.getLogger("parser_service")


@dataclass
class ParsedPage:
    """Represents a single parsed document page."""
    page_number: int
    content: str
    has_tables: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ParseResult:
    """Aggregated result of the document parsing process."""
    file_name: str
    total_pages: int
    full_markdown: str
    pages: List[ParsedPage]
    metadata: Dict[str, Any] = field(default_factory=dict)


class DocumentParserService:
    """
    Parses complex layout PDFs, balance sheets, tables, and financial reports
    into clean Markdown with structured page annotations.
    """

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or settings.LLAMA_CLOUD_API_KEY
        if not self.api_key:
            logger.warning("[PARSER INIT] LLAMA_CLOUD_API_KEY is not set. Parsing may fail.")

    def _get_parser(self) -> LlamaParse:
        """Initializes LlamaParse with high-fidelity Markdown table extraction."""
        return LlamaParse(
            api_key=self.api_key,
            result_type="markdown",
            split_by_page=True,
            adaptive_long_table=True,
            compact_markdown_table=True,
            language="en",
            verbose=False,
        )

    def _detect_tables(self, markdown_text: str) -> bool:
        """Checks if the page contains Markdown table syntax (| --- |)."""
        table_pattern = re.compile(r"\|[\s:\-]+\|", re.MULTILINE)
        return bool(table_pattern.search(markdown_text))

    async def parse_bytes(
        self,
        file_bytes: bytes,
        file_name: str,
    ) -> ParseResult:
        """
        Parses raw document bytes into page-segmented Markdown.
        - `file_bytes`: Raw binary content of the PDF.
        - `file_name`: Original name of the document.
        """
        logger.info("[PARSER] Starting LlamaParse for file '%s' (%d bytes)...", file_name, len(file_bytes))

        # Determine file extension
        ext = os.path.splitext(file_name)[1] or ".pdf"

        # Write bytes to a temporary file
        temp_file = tempfile.NamedTemporaryFile(mode="wb", suffix=ext, delete=False)
        temp_path = temp_file.name
        try:
            temp_file.write(file_bytes)
            temp_file.close()

            parser = self._get_parser()
            # Asynchronously parse document via LlamaCloud
            llama_docs = await parser.aload_data(temp_path)

            pages: List[ParsedPage] = []
            full_md_parts: List[str] = []

            for idx, doc in enumerate(llama_docs, start=1):
                page_text = doc.text or ""
                has_tables = self._detect_tables(page_text)
                
                # Extract page label if provided in doc.metadata
                page_num = idx
                if doc.metadata and "page_label" in doc.metadata:
                    try:
                        page_num = int(doc.metadata["page_label"])
                    except (ValueError, TypeError):
                        page_num = idx

                parsed_page = ParsedPage(
                    page_number=page_num,
                    content=page_text.strip(),
                    has_tables=has_tables,
                    metadata=doc.metadata or {},
                )
                pages.append(parsed_page)
                if page_text.strip():
                    full_md_parts.append(f"<!-- Page {page_num} -->\n{page_text.strip()}")

            full_markdown = "\n\n".join(full_md_parts)
            total_pages = len(pages) if pages else 1

            logger.info("[PARSER SUCCESS] Parsed '%s': %d pages extracted.", file_name, total_pages)

            return ParseResult(
                file_name=file_name,
                total_pages=total_pages,
                full_markdown=full_markdown,
                pages=pages,
                metadata={"byte_size": len(file_bytes), "page_count": total_pages},
            )

        except Exception as err:
            logger.error("[PARSER ERROR] Failed to parse '%s': %s", file_name, err)
            raise RuntimeError(f"LlamaParse extraction failed for '{file_name}': {err}") from err

        finally:
            # Always clean up temporary file
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception:
                    pass


parser_service = DocumentParserService()
