from __future__ import annotations

import re

_RE_EMAIL = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
_RE_BEARER = re.compile(r"\bBearer\s+[A-Za-z0-9\-._~+/]+=*", re.IGNORECASE)
_RE_CARD = re.compile(r"\b(?:\d[ -]?){13,19}\b")


def redact_pii(text: str) -> str:
    text = _RE_EMAIL.sub("[REDACTED_EMAIL]", text)
    text = _RE_BEARER.sub("Bearer [REDACTED_TOKEN]", text)
    text = _RE_CARD.sub("[REDACTED_CARD]", text)
    return text
