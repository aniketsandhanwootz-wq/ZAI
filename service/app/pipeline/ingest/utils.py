from __future__ import annotations

from typing import List


def chunk_text(text: str, max_chars: int = 900) -> List[str]:
    """
    Simple chunking: paragraph split + max char window.
    Good enough for MVP; can upgrade later.
    """
    text = (text or "").strip()
    if not text:
        return []

    paras = [p.strip() for p in text.split("\n") if p.strip()]
    chunks: List[str] = []
    buf = ""

    for p in paras:
        if len(buf) + len(p) + 1 <= max_chars:
            buf = (buf + "\n" + p).strip()
        else:
            if buf:
                chunks.append(buf)
            # if paragraph itself is huge, hard-split
            if len(p) > max_chars:
                for i in range(0, len(p), max_chars):
                    chunks.append(p[i : i + max_chars])
                buf = ""
            else:
                buf = p

    if buf:
        chunks.append(buf)
    return chunks
