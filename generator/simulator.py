"""Session-shaped shop traffic.

The generator assigns session_id. The pipeline does not compute sessions; that
is an M2 job (gap-based session windows on event_time). What this buys M1 is a
realistic funnel: one user, one device, one UTM, a cart that turns into an
order, and a payment that points at that order.

A session is a small state machine, not a clock. Wall time between events is
whatever EVENTS_PER_SECOND produces, so do not treat these session_ids as the
answer to "what is a session".
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

DEVICES: tuple[dict[str, str], ...] = (
    {"type": "mobile", "os": "ios", "app_version": "6.4.0"},
    {"type": "mobile", "os": "android", "app_version": "6.4.1"},
    {"type": "desktop", "os": "macos", "app_version": "web"},
    {"type": "desktop", "os": "windows", "app_version": "web"},
)

CAMPAIGNS: tuple[dict[str, str], ...] = (
    {"source": "google", "medium": "cpc", "campaign": "summer_sale"},
    {"source": "instagram", "medium": "paid_social", "campaign": "new_drop"},
    {"source": "newsletter", "medium": "email", "campaign": "weekly"},
    {"source": "google", "medium": "organic", "campaign": "brand"},
)

PAYMENT_METHODS: tuple[str, ...] = ("card", "card", "card", "paypal", "apple_pay")


def _now() -> str:
    # Milliseconds, UTC, Z suffix. Matches the schema description.
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _money(amount: Decimal) -> float:
    return float(amount.quantize(Decimal("0.01")))


def order_total(items: list[dict[str, Any]]) -> float:
    total = sum(
        (Decimal(str(item["unit_price"])) * int(item["qty"]) for item in items),
        Decimal("0"),
    )
    return _money(total)


@dataclass
class _Session:
    user_id: str
    session_id: str
    device: dict[str, str]
    utm: dict[str, str] | None
    currency: str
    cart: list[dict[str, Any]] = field(default_factory=list)
    last_url: str | None = None
    order_id: str | None = None
    order_total: float | None = None
    state: str = "browse"
    page_views: int = 0


class ShopSimulator:
    def __init__(
        self,
        catalog: list[dict[str, Any]],
        rng: random.Random | None = None,
        user_count: int = 80,
        max_sessions: int = 25,
    ) -> None:
        if not catalog:
            raise ValueError("catalog is empty")
        self._catalog = catalog
        self._rng = rng or random.Random()
        self._users = [f"u_{i:04d}" for i in range(1, user_count + 1)]
        self._max_sessions = max_sessions
        self._sessions: dict[str, _Session] = {}

    def next_event(self) -> dict[str, Any]:
        start_new = not self._sessions or (
            len(self._sessions) < self._max_sessions and self._rng.random() < 0.22
        )
        if start_new:
            return self._start_session()
        session = self._rng.choice(list(self._sessions.values()))
        return self._advance(session)

    def _start_session(self) -> dict[str, Any]:
        user_id = self._rng.choice(self._users)
        # One open session per user. A new visit replaces the abandoned one.
        self._sessions.pop(user_id, None)
        product = self._rng.choice(self._catalog)
        utm = None if self._rng.random() < 0.35 else dict(self._rng.choice(CAMPAIGNS))
        session = _Session(
            user_id=user_id,
            session_id="ses_" + uuid.uuid4().hex[:12],
            device=dict(self._rng.choice(DEVICES)),
            utm=utm,
            currency=str(product["currency"]),
        )
        self._sessions[user_id] = session
        return self._page_view(session, url="/", referrer=None)

    def _advance(self, session: _Session) -> dict[str, Any]:
        roll = self._rng.random()
        if session.state == "browse":
            # Cap browse length so a session reaches the cart instead of paging forever.
            if session.page_views >= 6:
                return self._add_to_cart(session)
            if roll < 0.62:
                return self._page_view(session)
            if roll < 0.90:
                return self._add_to_cart(session)
            return self._abandon(session)
        if session.state == "cart":
            if roll < 0.35:
                return self._add_to_cart(session)
            if roll < 0.55:
                return self._page_view(session, url="/cart")
            if roll < 0.88:
                return self._order(session)
            return self._abandon(session)
        if roll < 0.75:
            return self._payment(session)
        return self._abandon(session)

    def _abandon(self, session: _Session) -> dict[str, Any]:
        self._sessions.pop(session.user_id, None)
        return self._start_session()

    def _page_view(
        self,
        session: _Session,
        url: str | None = None,
        referrer: str | None = None,
    ) -> dict[str, Any]:
        if url is None:
            product = self._rng.choice(self._catalog)
            if self._rng.random() < 0.45:
                url = f"/c/{product['category']}"
            else:
                url = f"/p/{product['sku']}"
        page: dict[str, str] = {"url": url}
        ref = session.last_url if referrer is None else referrer
        if ref:
            page["referrer"] = ref
        session.last_url = url
        session.page_views += 1
        return self._event(session, "page_view", {"page": page})

    def _add_to_cart(self, session: _Session) -> dict[str, Any]:
        product = self._rng.choice(self._catalog)
        qty = self._rng.randint(1, 3)
        line = {
            "sku": product["sku"],
            "name": product["name"],
            "category": product["category"],
            "qty": qty,
            "unit_price": float(product["price"]),
        }
        merged = False
        for existing in session.cart:
            if existing["sku"] == line["sku"]:
                existing["qty"] += qty
                merged = True
                break
        if not merged:
            session.cart.append(dict(line))
        session.state = "cart"
        url = f"/p/{line['sku']}"
        page: dict[str, str] = {"url": url}
        if session.last_url:
            page["referrer"] = session.last_url
        session.last_url = url
        return self._event(session, "add_to_cart", {"page": page, "product": line})

    def _order(self, session: _Session) -> dict[str, Any]:
        items = [dict(item) for item in session.cart]
        total = order_total(items)
        session.order_id = "ord_" + uuid.uuid4().hex[:12]
        session.order_total = total
        session.state = "ordered"
        session.last_url = "/checkout"
        return self._event(
            session,
            "order_created",
            {
                "page": {"url": "/checkout", "referrer": "/cart"},
                "order": {
                    "order_id": session.order_id,
                    "currency": session.currency,
                    "items": items,
                    "total_amount": total,
                },
            },
        )

    def _payment(self, session: _Session) -> dict[str, Any]:
        if session.order_id is None or session.order_total is None:
            raise RuntimeError("payment without an order")
        event = self._event(
            session,
            "payment_completed",
            {
                "page": {"url": "/checkout/confirmation", "referrer": "/checkout"},
                "payment": {
                    "payment_id": "pay_" + uuid.uuid4().hex[:12],
                    "order_id": session.order_id,
                    "amount": session.order_total,
                    "method": self._rng.choice(PAYMENT_METHODS),
                    "status": "captured",
                },
            },
        )
        self._sessions.pop(session.user_id, None)
        return event

    def _event(self, session: _Session, event_type: str, extra: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {"device": dict(session.device)}
        if session.utm is not None:
            payload["utm"] = dict(session.utm)
        payload.update(extra)
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_time": _now(),
            "user_id": session.user_id,
            "session_id": session.session_id,
            "payload": payload,
        }
