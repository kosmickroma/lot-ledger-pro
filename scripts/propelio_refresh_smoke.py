"""Smoke test for the Refresh fix.

Calls _run_by_polygon directly with the exact shape the Refresh endpoint
would build, then re-queries propelio_comps to confirm the 5 previously-
missing pendings now exist in the cache.

Before fix: even with use_cache=False, _run_by_polygon could short-circuit
via the PHASE_2_CACHE_READ early-return path. After fix: that path now
respects use_cache.
"""

from __future__ import annotations

import asyncio
import sys

from api.config import get_session_conn, release_session_conn
from api.propelio.routes import PolygonRequest, _run_by_polygon

LEAD_AREA_ID = "98df299a-9381-4fa5-a119-db969106f43b"
MISSING_PENDING_ADDRS = [
    "3212 San Marcus Ave",
    "3130 Healey Dr",
    "4517 Sherwood Dr",
    "4238 Ridgedale Dr",
    "4404 San Marcus Dr",
]


def _missing_pending_status() -> dict[str, str]:
    """Return current status in propelio_comps for each missing pending addr."""
    out: dict[str, str] = {}
    c = get_session_conn()
    try:
        with c.cursor() as cur:
            for addr in MISSING_PENDING_ADDRS:
                cur.execute(
                    """
                    SELECT mls, status, last_seen_at::timestamp(0)
                    FROM propelio_comps
                    WHERE address ILIKE %s
                    ORDER BY last_seen_at DESC LIMIT 1
                    """,
                    (f"%{addr}%",),
                )
                row = cur.fetchone()
                if row:
                    out[addr] = f"present (mls={row[0]} status={row[1]} last_seen={row[2]})"
                else:
                    out[addr] = "ABSENT"
    finally:
        release_session_conn(c)
    return out


def _load_polygon(saved_area_id: str) -> list[list[float]]:
    c = get_session_conn()
    try:
        with c.cursor() as cur:
            cur.execute(
                "SELECT polygon FROM saved_areas WHERE area_id = %s",
                (saved_area_id,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"saved_area {saved_area_id} not found")
            return [[float(p[0]), float(p[1])] for p in row[0]]
    finally:
        release_session_conn(c)


async def main() -> int:
    print("=" * 78)
    print("Refresh fix smoke test — 2451 Crest Ridge polygon, months=1, range=1")
    print("=" * 78)

    print("\nBEFORE — current state of the 5 missing pendings in propelio_comps:")
    before = _missing_pending_status()
    for addr, status in before.items():
        print(f"  {addr:24s}  {status}")

    polygon = _load_polygon(LEAD_AREA_ID)
    print(f"\nLoaded polygon: {len(polygon)} vertices")

    req = PolygonRequest(
        polygon=polygon,
        months=1,
        range_override_mi=1.0,
        saved_area_id=LEAD_AREA_ID,
    )

    print("\nCalling _run_by_polygon(use_cache=False) …")
    result = await _run_by_polygon(req, use_cache=False)

    comps = result.get("comps") or []
    pendings = [c for c in comps if str(c.get("status") or "").lower() == "pending"]
    cma = result.get("cma_settings") or {}
    polymeta = result.get("polygon_meta") or {}

    print(
        f"\nResult: comps_pulled={polymeta.get('comps_pulled')}  "
        f"comps_in_polygon={polymeta.get('comps_in_polygon')}  "
        f"pendings_returned={len(pendings)}"
    )
    if cma:
        print(
            f"CMA params echoed: months={cma.get('params', {}).get('months') if isinstance(cma.get('params'), dict) else 'n/a'}  "
            f"sales_count={cma.get('sales_count')}"
        )

    print("\nPendings in the response:")
    for p in pendings:
        addr = p.get("address") or "?"
        print(f"  pending: {addr}")

    print("\nAFTER — state of the 5 missing pendings in propelio_comps:")
    after = _missing_pending_status()
    for addr, status in after.items():
        before_state = before[addr]
        change = "↑ NEW" if before_state == "ABSENT" and "present" in status else "(no change)"
        print(f"  {addr:24s}  {status}  {change}")

    print("\n" + "=" * 78)
    fresh_count = sum(
        1 for addr in MISSING_PENDING_ADDRS
        if before[addr] == "ABSENT" and "present" in after[addr]
    )
    if fresh_count == 5:
        print("✅ ALL 5 previously-missing pendings are now in propelio_comps.")
    elif fresh_count > 0:
        print(f"⚠ {fresh_count}/5 pendings now in cache — partial fix.")
    else:
        print("❌ NO pendings landed in propelio_comps. Fix didn't work.")
    print("=" * 78)
    return 0 if fresh_count > 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
