from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PlaceRecord:
    cid: str
    name: str | None
    website: str | None
    phone: str | None
    rating: float | None
    review_count: int | None
    status: str | None
    source: str | None = None
