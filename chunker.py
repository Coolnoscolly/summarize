import re
from typing import List

try:
    from chonkie import Chonkie
    CHONKIE_AVAILABLE = True
except ImportError:
    CHONKIE_AVAILABLE = False


class SmartChunker:
    """Умный чанкер с сохранением смысловых границ"""
    def __init__(self, max_chunk_size: int = 5000, overlap: int = 200):
        self.max_chunk_size = max_chunk_size
        self.overlap = overlap
        if CHONKIE_AVAILABLE:
            self.chonkie = Chonkie(chunk_size=max_chunk_size, chunk_overlap=overlap)
        else:
            self.chonkie = None

    def chunk_text(self, text: str) -> List[str]:
        """Разбивает текст на чанки с сохранением смысловых границ"""
        cleaned_text = re.sub(r"\s+", " ", text.strip())
        if self.chonkie is not None:
            return self.chonkie.split_text(cleaned_text)
        else:
            return self._split_text_manual(cleaned_text)

    def _split_text_manual(self, text: str) -> List[str]:
        """Ручная реализация разбиения текста на чанки"""
        chunks = []
        start = 0
        text_length = len(text)
        separators = ["\n\n", "\n", ". ", "! ", "? ", "; ", ", ", " "]
        while start < text_length:
            end = min(start + self.max_chunk_size, text_length)
            if end == text_length:
                chunk = text[start:end]
                chunks.append(chunk)
                break
            best_break_pos = end
            for separator in separators:
                pos = text.rfind(separator, start, end)
                if pos != -1 and pos > start + (self.max_chunk_size * 0.6):
                    best_break_pos = pos + len(separator)
                    break
            chunk = text[start:best_break_pos]
            chunks.append(chunk)
            start = best_break_pos - self.overlap if best_break_pos > self.overlap else best_break_pos
        return chunks

    def chunk_document(self, content: str, min_chunk_size: int = 100) -> List[str]:
        """Разбивает документ на чанки и оптимизирует в один чанк"""
        chunks = self.chunk_text(content)
        merged_chunks = []
        current_chunk = ""
        for chunk in chunks:
            chunk = chunk.strip()
            if not chunk:
                continue
            if len(current_chunk) + len(chunk) <= self.max_chunk_size:
                current_chunk += " " + chunk if current_chunk else chunk
            else:
                if current_chunk and len(current_chunk) >= min_chunk_size:
                    merged_chunks.append(current_chunk)
                current_chunk = chunk
        if current_chunk and len(current_chunk) >= min_chunk_size:
            merged_chunks.append(current_chunk)
        if merged_chunks:
            optimized_chunk = "\n\n".join(merged_chunks)
            if len(optimized_chunk) > self.max_chunk_size:
                optimized_chunk = optimized_chunk[:self.max_chunk_size]
            return [optimized_chunk]
        return []