from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CorpRecord:
    corporate_number: str
    name: str
    kind: str | None
    prefecture: str | None
    city: str | None
    address: str | None
    updated_at: str | None
    source: str | None = None
