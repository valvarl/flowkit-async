from __future__ import annotations
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Protocol, Tuple

from ..io.content import ContentKind, FrameDescriptor
from ..worker.handlers.base import Batch


__all__ = [
    "Codec",
    "CodecsRegistry",
    "get_default_codecs",
    "JsonLinesCodec",
]


class Codec(Protocol):
    """Протокол кодека для фреймированных форматов.

    Кодек обязан быть «чистым» (без побочных эффектов) и потокобезопасным.
    """

    name: str

    def can_handle(self, kind: ContentKind, frame: FrameDescriptor | None) -> bool: ...
    def encode(self, batch: Batch) -> Tuple[bytes, FrameDescriptor]:
        """Преобразует batch.items (или batch.blob) в (blob, frame).

        Должен:
          - работать только для batch.content_kind in {BATCH, BYTES}
          - заполнять descriptor (если user не указал frame)
        """
        ...

    def decode(self, blob: bytes, frame: FrameDescriptor) -> Batch:
        """Создаёт Batch из блоба (обычно BATCH/RECORD).

        Обычно возвращает batch с .items и content_kind=BATCH (или RECORD).
        """
        ...


class JsonLinesCodec:
    """Простой JSONL кодек (строки разделены '\n')."""

    name = "jsonl"

    def can_handle(self, kind: ContentKind, frame: FrameDescriptor | None) -> bool:
        if frame is None:
            return False
        return frame.kind.lower() in ("jsonl", "json_lines", "ndjson")

    def encode(self, batch: Batch) -> Tuple[bytes, FrameDescriptor]:
        if batch.content_kind not in (ContentKind.BATCH, ContentKind.RECORD):
            raise ValueError("jsonl.encode: ожидается BATCH|RECORD")
        if batch.items:
            lines = (json.dumps(x, ensure_ascii=False) for x in batch.items)
            payload = ("\n".join(lines) + "\n").encode("utf-8")
            frame = batch.frame or FrameDescriptor(kind="jsonl", encoding="utf-8")
            return payload, frame
        # допускается RECORD в blob? — здесь считаем, что нет
        raise ValueError("jsonl.encode: batch.items пуст")

    def decode(self, blob: bytes, frame: FrameDescriptor) -> Batch:
        text = blob.decode(frame.encoding or "utf-8")
        items = []
        for line in text.splitlines():
            if not line.strip():
                continue
            items.append(json.loads(line))
        from ..worker.handlers.base import Batch as _Batch  # локальный импорт, чтобы избежать циклов

        return _Batch(content_kind=ContentKind.BATCH, items=items)


@dataclass
class _Entry:
    codec: Codec


class CodecsRegistry:
    """Реестр кодеков: поиск по frame/kind, удобные врапперы encode/decode."""

    def __init__(self) -> None:
        self._entries: list[_Entry] = []

    def register(self, codec: Codec) -> None:
        # простая проверка уникальности по имени
        names = {e.codec.name for e in self._entries}
        if codec.name in names:
            # last-wins: перерегистрируем
            self._entries = [e for e in self._entries if e.codec.name != codec.name]
        self._entries.append(_Entry(codec))

    def find(self, kind: ContentKind, frame: FrameDescriptor | None) -> Codec | None:
        for e in self._entries:
            if e.codec.can_handle(kind, frame):
                return e.codec
        return None

    # Удобные шорткаты

    def encode_to_blob(self, batch: Batch) -> Tuple[bytes, FrameDescriptor]:
        """Возвращает (blob, frame), поднимая исключение если подходящего кодека нет."""
        codec = self.find(batch.content_kind, batch.frame)
        if not codec:
            raise LookupError(f"no codec for kind={batch.content_kind} frame={batch.frame}")
        return codec.encode(batch)

    def decode_from_blob(self, blob: bytes, frame: FrameDescriptor) -> Batch:
        codec = self.find(ContentKind.BATCH, frame)
        if not codec:
            raise LookupError(f"no codec for frame={frame.kind}")
        return codec.decode(blob, frame)


_default_registry: CodecsRegistry | None = None


def get_default_codecs() -> CodecsRegistry:
    global _default_registry
    if _default_registry is None:
        reg = CodecsRegistry()
        reg.register(JsonLinesCodec())
        _default_registry = reg
    return _default_registry
