from typing import List, Tuple, Optional
from config.settings import settings
from core.loader import HybridLoader
from core.chunker import SmartChunker
from core.summarizer import OllamaSummarizer
from core.merger import HierarchicalMerger
from utils.helpers import format_final_summary


class SummaryPipeline:
    """Высокоуровневый пайплайн суммаризации"""
    def __init__(
        self,
        settings_obj=None,
        loader: Optional[HybridLoader] = None,
        chunker: Optional[SmartChunker] = None,
        summarizer: Optional[OllamaSummarizer] = None,
        merger: Optional[HierarchicalMerger] = None,
    ) -> None:
        self.settings = settings_obj or settings
        self.loader = loader or HybridLoader()
        self.chunker = chunker or SmartChunker(
            max_chunk_size=self.settings.MAX_CHUNK_SIZE,
            overlap=self.settings.CHUNK_OVERLAP,
        )
        self.summarizer = summarizer or OllamaSummarizer()
        self.merger = merger or HierarchicalMerger(
            self.summarizer, max_workers=self.settings.MAX_WORKERS
        )

    def run(self, save_to: Optional[str] = None, file_list: Optional[List[str]] = None) -> str:
        """Запускает пайплайн"""
        return self.summarize_minio(save_to=save_to, file_list=file_list)

    def summarize_minio(self, save_to: Optional[str] = None, file_list: Optional[List[str]] = None) -> str:

        if file_list and len(file_list) == 1:
            documents = self.loader.load_documents(file_list=file_list)
            if documents:
                filename, content = documents[0]
                preview = content[:100] + "..." if len(content) > 100 else content
            return preview

        documents = self.loader.load_documents(file_list=file_list)
        return self._summarize_documents(documents, save_to=save_to)

    def summarize_texts(self, texts: List[str], save_to: Optional[str] = None) -> str:
        documents: List[Tuple[str, str]] = [
            ("", t) for t in texts if t and isinstance(t, str) and t.strip()
        ]
        return self._summarize_documents(documents, save_to=save_to)

    def summarize_documents(
        self, documents: List[Tuple[str, str]], save_to: Optional[str] = None
    ) -> str:
        return self._summarize_documents(documents, save_to=save_to)

    def _summarize_documents(
        self, documents: List[Tuple[str, str]], save_to: Optional[str] = None
    ) -> str:
        if not documents:
            return ""
        all_chunks: List[str] = []
        for _, content in documents:
            chunks = self.chunker.chunk_document(content)
            if chunks:
                all_chunks.extend(chunks)
        if not all_chunks:
            return ""
        final_summary = self.merger.merge_documents(all_chunks)
        formatted = format_final_summary(final_summary, self.settings.FINAL_STYLE)
        if save_to is None and getattr(self.settings, "OUTPUT_FILE", None):
            save_to = self.settings.OUTPUT_FILE
        if save_to:
            with open(save_to, "w", encoding="utf-8") as f:
                f.write(formatted)
        return formatted