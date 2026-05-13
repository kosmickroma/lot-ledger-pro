# api/propelio/routes.py
#
# FastAPI router for Propelio per-address comp fetches.
# Handles cache lookup/write, quota logging, and response normalization.
#
# Connects to:
#   api/propelio/scraper.py  - synchronous Propelio client call
#   api/propelio/cache.py    - address cache and quota log helpers

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import secrets
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import requests
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

from api.auth import get_current_user
from api.config import get_conn, get_session_conn, release_conn, release_session_conn
from api.geo import haversine_miles, point_in_polygon, polygon_centroid
from api.redfin import normalize_addr_key

from .archive import load_archived_comps, load_comps_by_polygon, merge_comps_into_archive, merge_comps_into_global, set_comp_rating
from .deep_pull import PASSES_RECENT, STALE_JOB_THRESHOLD_MINUTES, run_deep_pull
from .parcel_match import match_comps_to_parcels
from . import cache as cache_mod
from . import scraper as scraper_mod


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/propelio", tags=["propelio"])


_PROPELIO_PHOTO_PREFIX = "https://api.propelio.com/mls-media/"


class PolygonRequest(BaseModel):
    polygon: list[list[float]]
    months: int = 24
    range_override_mi: float | None = None
    saved_area_id: str | None = None


class RefreshRequest(BaseModel):
    saved_area_id: str
    months: int = 24
    range_override_mi: float | None = None


class DeepPullStartRequest(BaseModel):
    target_address: str
    saved_area_id: str | None = None


def _ensure_deep_pull_role(user: dict[str, Any]) -> None:
    role = str(user.get("role") or "").strip().lower()
    if role not in {"developer", "owner"}:
        raise HTTPException(status_code=403, detail="Deep pull is developer-only")


def _fetch_propelio_photo_response(photo_url: str):
    last_status: int | None = None

    for attempt in range(2):
        client = scraper_mod.login_propelio()
        try:
            response = client.session.get(
                photo_url,
                timeout=scraper_mod.config.HTTP_TIMEOUT_SECONDS,
                stream=True,
            )
        except requests.RequestException as exc:
            raise HTTPException(status_code=502, detail=f"Propelio photo fetch failed: {exc}") from exc

        if response.status_code == 200:
            return response

        last_status = int(response.status_code)
        response.close()

        if response.status_code in {401, 403} and attempt == 0:
            logger.info("Propelio photo fetch got %s; retrying after re-login", response.status_code)
            continue

        if response.status_code in {401, 403}:
            raise HTTPException(status_code=502, detail="Propelio photo auth failed after re-login")

        raise HTTPException(status_code=502, detail=f"Propelio photo upstream returned {response.status_code}")

    raise HTTPException(status_code=502, detail=f"Propelio photo upstream returned {last_status or 'unknown'}")


def _extract_balance(subject_extra: dict[str, Any]) -> int | None:
    if not isinstance(subject_extra, dict):
        return None

    def _as_int(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    search_roots = [
        subject_extra,
        subject_extra.get("valuation") if isinstance(subject_extra.get("valuation"), dict) else None,
        subject_extra.get("raw") if isinstance(subject_extra.get("raw"), dict) else None,
        subject_extra.get("withaddress") if isinstance(subject_extra.get("withaddress"), dict) else None,
    ]

    for root in search_roots:
        if not isinstance(root, dict):
            continue
        for key in ("balance", "remaining", "remainingConsumables", "consumablesRemaining"):
            got = _as_int(root.get(key))
            if got is not None:
                return got
    return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _first(*vals: Any) -> Any:
    for v in vals:
        if v not in (None, "", 0):
            return v
    return None


def _build_payload(subject: Any, comps_list: list[Any]) -> dict[str, Any]:
    subject_dict = asdict(subject)
    subject_extra = subject_dict.pop("extra", {}) if isinstance(subject_dict.get("extra"), dict) else {}
    parcel_enrichment = subject_extra.get("parcel_enrichment") or {}
    valuation = subject_extra.get("valuation") or {}
    return {
        "fetched_at": _now_iso(),
        "balance": _extract_balance(subject_extra),
        "cma_settings": {
            "params": subject_extra.get("cma_params"),
            "arv": subject_extra.get("cma_arv"),
            "arv_type": subject_extra.get("cma_arvType"),
            "as_of_dt": subject_extra.get("cma_as_of_dt"),
            "start_dt": subject_extra.get("cma_start_dt"),
            "sales_count": subject_extra.get("cma_sales_count"),
            "leases_count": subject_extra.get("cma_leases_count"),
            "cma_id": subject_extra.get("cma_id"),
        },
        "subject": {
            "address": subject_dict.get("address"),
            "lot_size": _first(subject_dict.get("lot_size"), parcel_enrichment.get("lot_size")),
            "sqft": _first(subject_dict.get("sqft"), parcel_enrichment.get("sqft")),
            "year_built": _first(subject_dict.get("year_built"), parcel_enrichment.get("year_built")),
            "neighborhood": _first(subject_dict.get("neighborhood"), parcel_enrichment.get("subdivision")),
            "lat": _first(subject_extra.get("lat"), parcel_enrichment.get("lat")),
            "lon": _first(subject_extra.get("lon"), parcel_enrichment.get("lon")),
            "parcel_enrichment": parcel_enrichment or None,
            "valuation": valuation or None,
            "transfer_history": subject_extra.get("transfer_history") or subject_extra.get("transfers"),
            "raw": subject_extra.get("raw"),
        },
        "comps": [asdict(comp) for comp in comps_list],
    }


def _validate_polygon(points: list[list[float]]) -> list[list[float]]:
    if len(points) < 3:
        raise HTTPException(status_code=400, detail="Polygon must contain at least three points")

    normalized: list[list[float]] = []
    for point in points:
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            raise HTTPException(status_code=400, detail="Polygon points must be [lng, lat] pairs")
        lng = float(point[0])
        lat = float(point[1])
        if not -180.0 <= lng <= 180.0:
            raise HTTPException(status_code=400, detail="Polygon lng must be between -180 and 180")
        if not -90.0 <= lat <= 90.0:
            raise HTTPException(status_code=400, detail="Polygon lat must be between -90 and 90")
        normalized.append([lng, lat])
    return normalized


def _polygon_cache_key(polygon: list[list[float]], months: int, range_mi: float) -> str:
    canonical = json.dumps({"v": 2, "polygon": polygon, "months": months, "range_mi": round(float(range_mi), 6)}, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _nearest_subject_parcel(lat: float, lng: float) -> dict[str, Any] | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH point AS (
                    SELECT ST_SetSRID(ST_Point(%s, %s), 4326) AS geom
                ),
                candidates AS (
                    SELECT
                        'dcad'::text AS county,
                        p.account_num,
                        p.property_address AS address,
                        ST_Distance(p.centroid::geography, point.geom::geography) AS distance_m
                    FROM parcels p, point
                    WHERE p.centroid IS NOT NULL
                      AND p.property_address IS NOT NULL
                      AND p.property_address <> ''

                    UNION ALL

                    SELECT
                        'tad'::text AS county,
                        t.account_num,
                        t.situs_addr AS address,
                        ST_Distance(t.centroid::geography, point.geom::geography) AS distance_m
                    FROM tad_parcels t, point
                    WHERE t.centroid IS NOT NULL
                      AND t.situs_addr IS NOT NULL
                      AND t.situs_addr <> ''

                    UNION ALL

                    SELECT
                        'collin'::text AS county,
                        c.account_num,
                        c.property_address AS address,
                        ST_Distance(c.centroid::geography, point.geom::geography) AS distance_m
                    FROM collin_parcels c, point
                    WHERE c.centroid IS NOT NULL
                      AND c.property_address IS NOT NULL
                      AND c.property_address <> ''

                    UNION ALL

                    SELECT
                        'denton'::text AS county,
                        d.account_num,
                        d.property_address AS address,
                        ST_Distance(d.centroid::geography, point.geom::geography) AS distance_m
                    FROM denton_parcels d, point
                    WHERE d.centroid IS NOT NULL
                      AND d.property_address IS NOT NULL
                      AND d.property_address <> ''
                )
                SELECT county, account_num, address, distance_m
                FROM candidates
                ORDER BY distance_m ASC
                LIMIT 1
                """,
                (lng, lat),
            )
            row = cur.fetchone()
            if row is None:
                return None
            distance_m = float(row[3] or 0.0)
            if distance_m > 1609.344:
                return None
            return {
                "county": str(row[0]),
                "account_num": str(row[1]),
                "address": str(row[2]),
                "distance_m": distance_m,
                "address_key": normalize_addr_key(row[2]),
            }
    finally:
        release_conn(conn)


def _comp_point(comp: Any) -> tuple[float | None, float | None]:
    extra = comp.extra if isinstance(getattr(comp, "extra", None), dict) else {}
    lat_raw = extra.get("lat")
    lng_raw = extra.get("lon", extra.get("lng"))
    try:
        lat = float(lat_raw)
        lng = float(lng_raw)
    except (TypeError, ValueError):
        return None, None
    return lat, lng


def _load_saved_area_polygon(saved_area_id: str) -> list[list[float]]:
    normalized = str(saved_area_id or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="saved_area_id is required")

    conn = get_session_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT polygon
                FROM saved_areas
                WHERE area_id = %s
                LIMIT 1
                """,
                (normalized,),
            )
            row = cur.fetchone()
    finally:
        release_session_conn(conn)

    if row is None:
        raise HTTPException(status_code=404, detail="saved_area_id not found")

    polygon = row[0]
    if not isinstance(polygon, list):
        raise HTTPException(status_code=422, detail="Saved area polygon is invalid")
    return _validate_polygon(polygon)


async def _run_by_polygon(request: PolygonRequest, *, use_cache: bool, cache_only: bool = False) -> dict[str, Any]:
    polygon = _validate_polygon(request.polygon)
    months = int(request.months)
    saved_area_id = str(request.saved_area_id or "").strip() or None

    # --- Phase 2 Chunk 3: global cache read (gated by PHASE_2_CACHE_READ env var) ---
    # Bug fix 2026-05-13: this block was ignoring the caller's `use_cache` flag,
    # so the Refresh endpoint (which sets use_cache=False precisely to bypass
    # cache and re-pull from Propelio) was being silently short-circuited
    # whenever PHASE_2_CACHE_READ was on. Honor use_cache here too.
    if use_cache and os.environ.get("PHASE_2_CACHE_READ") == "true":
        try:
            cached_global = load_comps_by_polygon(polygon, saved_area_id)
        except Exception as _exc:
            logger.warning("[propelio] global cache read failed (non-fatal): %s", _exc)
            cached_global = []
        _centroid_lat, _centroid_lng = polygon_centroid(polygon)

        subject_parcel = _nearest_subject_parcel(_centroid_lat, _centroid_lng)
        subject: dict[str, Any] | None = None
        if subject_parcel:
            subject = {
                "address": subject_parcel["address"],
                "county": subject_parcel["county"],
                "account_num": subject_parcel["account_num"],
            }

        comps_in_polygon = sum(
            1
            for comp in cached_global
            if not (
                isinstance(comp.get("extra"), dict)
                and comp["extra"].get("is_outside_polygon")
            )
        )
        comps_outside_polygon = len(cached_global) - comps_in_polygon

        polygon_meta: dict[str, Any] = {
            "centroid": {"lat": _centroid_lat, "lng": _centroid_lng},
            "comps_in_polygon": comps_in_polygon,
            "comps_outside_polygon": comps_outside_polygon,
            "comps_pulled": len(cached_global),
            "subject_parcel": subject_parcel,
        }

        cma_settings: dict[str, Any] = {
            "months": int(request.months),
            "range": "(cached)" if cached_global else "(cache-only)",
            "sales_count": len(cached_global),
        }

        try:
            balance: int | None = cache_mod.latest_quota_balance() if cached_global else None
        except Exception:
            balance = None

        if cache_only:
            return {
                "cached": bool(cached_global),
                "cache_only": True,
                "comps": cached_global if cached_global else [],
                "polygon_meta": polygon_meta,
                "cma_settings": cma_settings,
                "balance": balance,
                "subject": subject,
            }

        if cached_global:
            logger.info(
                "[propelio] PHASE_2_CACHE_READ hit: %d comps for saved_area=%s",
                len(cached_global),
                saved_area_id,
            )
            ph2_archive_meta: dict[str, Any] | None = None
            if saved_area_id:
                try:
                    ph2_archive_meta = merge_comps_into_archive(saved_area_id, cached_global)
                except Exception as _ae:
                    logger.warning(
                        "[propelio] archive sync failed on cache-hit (non-fatal): %s", _ae
                    )

            response: dict[str, Any] = {
                "cached": True,
                "phase2_cache": True,
                "comps": cached_global,
                "polygon_meta": polygon_meta,
                "cma_settings": cma_settings,
                "balance": balance,
                "subject": subject,
            }
            if ph2_archive_meta is not None:
                response["archive_meta"] = ph2_archive_meta
            return response
    # --- end Chunk 3 gate ---

    centroid_lat, centroid_lng = polygon_centroid(polygon)
    circumradius_mi = max(
        haversine_miles(centroid_lat, centroid_lng, point_lat, point_lng)
        for point_lng, point_lat in polygon
    )
    computed_range_mi = min(circumradius_mi * 1.05, 10.0)
    if request.range_override_mi is None:
        range_mi = computed_range_mi
    else:
        range_mi = min(max(float(request.range_override_mi), 0.01), 10.0)

    cache_key = _polygon_cache_key(polygon, months, range_mi)
    if use_cache:
        cached = cache_mod.get_cached(cache_key)
        if cached is not None:
            cached_payload = dict(cached)
            cached_payload["comps"] = match_comps_to_parcels(cached_payload.get("comps") or [])
            polygon_meta = cached_payload.get("polygon_meta")
            if isinstance(polygon_meta, dict):
                comps_pulled = polygon_meta.get("comps_pulled")
                if not isinstance(comps_pulled, int):
                    comps_pulled = len(cached_payload.get("comps") or [])
                comps_in_polygon = polygon_meta.get("comps_in_polygon")
                if not isinstance(comps_in_polygon, int):
                    comps_in_polygon = 0
                polygon_meta["comps_outside_polygon"] = max(comps_pulled - comps_in_polygon, 0)
            if saved_area_id:
                cached_payload["archive_meta"] = merge_comps_into_archive(saved_area_id, cached_payload.get("comps") or [])
                # Reload the archive view so user_rating + comp_address_key
                # round-trip on cache hits, matching the fresh-pull path.
                cached_payload["comps"] = load_archived_comps(saved_area_id)
            try:
                merge_comps_into_global(cached_payload.get("comps") or [], "by_polygon")
            except Exception as _exc:
                logger.warning("[propelio] global write failed (cache-hit): %s", _exc)
            return {"cached": True, **cached_payload}

    subject_parcel = _nearest_subject_parcel(centroid_lat, centroid_lng)
    if subject_parcel is None:
        raise HTTPException(status_code=404, detail="No parcel found near polygon centroid")

    try:
        subject, comps_list = await asyncio.to_thread(
            scraper_mod.search_properties,
            subject_parcel["address"],
            months=months,
            range_mi=range_mi,
        )
    except scraper_mod.PropelioScraperError as exc:
        msg = str(exc)
        msg_lower = msg.lower()
        # Treat the "no MLS coverage in this area" upstream error the same way
        # as the empty-list case — it's not a service failure, just a data
        # limitation Propelio's surfacing for this polygon. Return 200 with
        # an empty comps payload + warning so the frontend can show a soft
        # message instead of a black-box 503 in the console.
        is_empty = "empty list" in msg_lower
        is_no_coverage = "mls_coverage_error" in msg_lower or "we don't have coverage" in msg_lower
        if is_empty or is_no_coverage:
            warning_text = (
                "Propelio reports no MLS coverage for this area. Try drawing inside a covered county "
                "(Dallas / Tarrant / Collin / Denton) or pick a higher-activity neighborhood."
                if is_no_coverage
                else "Propelio resolved the centroid parcel but returned 0 comps for the polygon pull under the current filter settings."
            )
            empty_payload = {
                "fetched_at": _now_iso(),
                "balance": None,
                "cma_settings": {
                    "params": None,
                    "arv": None,
                    "arv_type": None,
                    "as_of_dt": None,
                    "start_dt": None,
                    "sales_count": 0,
                    "leases_count": None,
                    "cma_id": None,
                },
                "subject": {
                    "address": subject_parcel["address"],
                    "lot_size": None,
                    "sqft": None,
                    "year_built": None,
                    "neighborhood": None,
                    "lat": None,
                    "lon": None,
                    "parcel_enrichment": None,
                    "valuation": None,
                    "transfer_history": None,
                    "raw": None,
                },
                "comps": [],
                "polygon_meta": {
                    "centroid": {"lat": centroid_lat, "lng": centroid_lng},
                    "circumradius_mi": circumradius_mi,
                    "subject_parcel": {
                        "address": subject_parcel["address"],
                        "county": subject_parcel["county"],
                        "account_num": subject_parcel["account_num"],
                    },
                    "comps_pulled": 0,
                    "comps_in_polygon": 0,
                    "comps_outside_polygon": 0,
                },
                "warning": warning_text,
            }
            if saved_area_id:
                empty_payload["archive_meta"] = merge_comps_into_archive(saved_area_id, [])
            try:
                merge_comps_into_global([], "by_polygon")
            except Exception as _exc:
                logger.warning("[propelio] global write failed (empty-comps): %s", _exc)
            return {"cached": False, **empty_payload}
        # Surface the scraper error in logs so we can see WHY Propelio
        # rejected the pull (auth expired, lead-id lookup failed, captcha,
        # network blip, etc.). The frontend currently throws away the
        # response body, so without this log the 503 is a black box.
        logger.warning("[propelio] by-polygon scraper error: %s", msg)
        raise HTTPException(
            status_code=503,
            detail=f"Propelio service unavailable: {msg}",
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Address not resolvable on Propelio") from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Unexpected Propelio by-polygon error")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {exc}",
        ) from exc

    comps_in_polygon = 0
    for comp in comps_list:
        lat, lng = _comp_point(comp)
        if lat is None or lng is None:
            continue
        if point_in_polygon(lat, lng, polygon):
            comps_in_polygon += 1

    payload = _build_payload(subject, comps_list)
    payload["comps"] = match_comps_to_parcels(payload.get("comps") or [])
    comps_pulled = len(comps_list)
    payload["polygon_meta"] = {
        "centroid": {"lat": centroid_lat, "lng": centroid_lng},
        "circumradius_mi": circumradius_mi,
        "subject_parcel": {
            "address": subject_parcel["address"],
            "county": subject_parcel["county"],
            "account_num": subject_parcel["account_num"],
        },
        "comps_pulled": comps_pulled,
        "comps_in_polygon": comps_in_polygon,
        "comps_outside_polygon": max(comps_pulled - comps_in_polygon, 0),
    }

    if saved_area_id:
        payload["archive_meta"] = merge_comps_into_archive(saved_area_id, payload.get("comps") or [])
        # Swap in the archive view of comps so the response includes any
        # user_rating that previously existed on these comps (preserved
        # across refreshes via the merge function). New unrated comps
        # get user_rating=None.
        payload["comps"] = load_archived_comps(saved_area_id)
    try:
        merge_comps_into_global(payload.get("comps") or [], "by_polygon")
    except Exception as _exc:
        logger.warning("[propelio] global write failed (fresh-scrape): %s", _exc)

    if use_cache:
        cache_payload = dict(payload)
        cache_payload.pop("archive_meta", None)
        cache_mod.log_quota(payload.get("balance"), cache_key)
        cache_mod.put_cached(cache_key, cache_payload)
    return {"cached": False, **payload}


@router.get("/by-address")
async def get_by_address(
    address: str = Query(..., min_length=3),
    months: int = Query(24, ge=1, le=60),
    range: float = Query(1.0, gt=0.0, le=10.0),
) -> dict[str, Any]:
    address_key_base = cache_mod.normalize_address_key(address)
    if not address_key_base:
        raise HTTPException(status_code=400, detail="Address is required")

    # Cache key includes filter params so different settings don't collide.
    # Only fresh CMA generations honor these params — existing CMAs return
    # cached data — but cache key still distinguishes for clarity.
    address_key = f"{address_key_base}|m{months}|r{range}"

    cached = cache_mod.get_cached(address_key)
    if cached is not None:
        cached_payload = dict(cached)
        cached_payload["comps"] = match_comps_to_parcels(cached_payload.get("comps") or [])
        return {"cached": True, **cached_payload}

    try:
        subject, comps_list = await asyncio.to_thread(
            scraper_mod.search_properties,
            address,
            months=months,
            range_mi=range,
        )
    except scraper_mod.PropelioScraperError as exc:
        msg = str(exc)
        msg_lower = msg.lower()
        # Empty CMA / lead-details lists AND "no MLS coverage" upstream errors
        # are not real failures — they mean Propelio either resolved the
        # address with no nearby comps in the time window OR doesn't license
        # MLS data in this geographic area. Return 200 with empty comps so
        # the frontend can show a soft message rather than an error toast.
        is_empty = "empty list" in msg_lower
        is_no_coverage = "mls_coverage_error" in msg_lower or "we don't have coverage" in msg_lower
        if is_empty or is_no_coverage:
            if is_no_coverage:
                logger.info("Propelio reports no MLS coverage for %r — returning 200 with no comps", address)
                warning_text = "Propelio reports no MLS coverage for this area. Try an address in Dallas / Tarrant / Collin / Denton county."
            else:
                logger.info("Propelio returned empty pool for %r — returning 200 with no comps", address)
                warning_text = "Propelio resolved this address but returned 0 comps under their default filter (typically 0.5mi · 6mo · similar lot size). Try a higher-activity neighborhood, or wait for the filter-widen feature."
            empty_payload = {
                "fetched_at": _now_iso(),
                "balance": None,
                "cma_settings": {
                    "params": None, "arv": None, "arv_type": None,
                    "as_of_dt": None, "start_dt": None,
                    "sales_count": 0, "leases_count": None, "cma_id": None,
                },
                "subject": {"address": address, "neighborhood": None, "lot_size": None,
                            "sqft": None, "year_built": None, "lat": None, "lon": None,
                            "parcel_enrichment": None, "valuation": None,
                            "transfer_history": None, "raw": None},
                "comps": [],
                "warning": warning_text,
            }
            return {"cached": False, **empty_payload}
        logger.warning("[propelio] by-address scraper error: %s", msg)
        raise HTTPException(
            status_code=503,
            detail=f"Propelio service unavailable: {msg}",
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="Address not resolvable on Propelio") from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Unexpected Propelio by-address error")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {exc}",
        ) from exc

    payload = _build_payload(subject, comps_list)
    payload["comps"] = match_comps_to_parcels(payload.get("comps") or [])

    try:
        merge_comps_into_global(payload.get("comps") or [], "by_address")
    except Exception as _exc:
        logger.warning("[propelio] global write failed (by-address): %s", _exc)

    cache_mod.log_quota(payload.get("balance"), address_key)
    cache_mod.put_cached(address_key, payload)
    return {"cached": False, **payload}


@router.get("/photo")
def get_photo(url: str = Query(..., min_length=1)) -> StreamingResponse:
    photo_url = str(url or "").strip()
    if not photo_url.startswith(_PROPELIO_PHOTO_PREFIX):
        raise HTTPException(status_code=400, detail="Invalid Propelio photo URL")

    response = _fetch_propelio_photo_response(photo_url)
    media_type = response.headers.get("Content-Type") or "application/octet-stream"
    return StreamingResponse(
        response.iter_content(chunk_size=64 * 1024),
        media_type=media_type,
        headers={"Cache-Control": "public, max-age=2419200, immutable"},
        background=BackgroundTask(response.close),
    )


@router.post("/by-polygon")
async def get_by_polygon(
    request: PolygonRequest,
    saved_area_id: str | None = Query(None),
    cache_only: bool = Query(False),
) -> dict[str, Any]:
    body_saved_area_id = str(request.saved_area_id or "").strip() or None
    query_saved_area_id = str(saved_area_id or "").strip() or None
    effective_saved_area_id = query_saved_area_id or body_saved_area_id

    effective_request = PolygonRequest(
        polygon=request.polygon,
        months=request.months,
        range_override_mi=request.range_override_mi,
        saved_area_id=effective_saved_area_id,
    )
    return await _run_by_polygon(effective_request, use_cache=True, cache_only=cache_only)


@router.post("/refresh")
async def refresh_by_saved_area(request: RefreshRequest) -> dict[str, Any]:
    saved_area_id = str(request.saved_area_id or "").strip()
    if not saved_area_id:
        raise HTTPException(status_code=400, detail="saved_area_id is required")

    polygon = _load_saved_area_polygon(saved_area_id)
    by_poly_request = PolygonRequest(
        polygon=polygon,
        months=int(request.months),
        range_override_mi=request.range_override_mi,
        saved_area_id=saved_area_id,
    )
    response = await _run_by_polygon(by_poly_request, use_cache=False)
    response["comps"] = load_archived_comps(saved_area_id)
    return response


@router.get("/by-saved-area")
async def get_by_saved_area(saved_area_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    normalized = str(saved_area_id or "").strip()
    if not normalized:
        raise HTTPException(status_code=400, detail="saved_area_id is required")

    # Phase 2: query the global propelio_comps cache via spatial lookup on the
    # saved area's polygon. Falls back to legacy propelio_comp_archive when
    # the area isn't a polygon type (location/parcel) or the spatial cache is
    # empty (pre-Phase-2 areas that were never deep-pulled).
    comps: list[dict[str, Any]] = []
    try:
        polygon = _load_saved_area_polygon(normalized)
        comps = load_comps_by_polygon(polygon, normalized)
    except HTTPException:
        comps = []

    if not comps:
        comps = load_archived_comps(normalized)

    return {"comps": comps}


@router.post("/deep-pull/start")
async def start_deep_pull(
    request: DeepPullStartRequest,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    address = str(request.target_address or "").strip()
    if not address:
        raise HTTPException(status_code=400, detail="target_address is required")
    saved_area_id = str(request.saved_area_id or "").strip() or None

    job_id = "dp_" + secrets.token_urlsafe(8)

    conn = get_session_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO propelio_deep_pull_jobs
                    (job_id, target_address, saved_area_id, started_by_user_id, status)
                VALUES (%s, %s, %s, %s, 'queued')
                """,
                (job_id, address, saved_area_id, int(user["id"])),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_session_conn(conn)

    asyncio.create_task(run_deep_pull(job_id))
    return {"job_id": job_id}


@router.post("/refresh-recent/start")
async def start_refresh_recent(
    request: DeepPullStartRequest,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    """
    Triggers a shorter, recency-focused deep-pull on the same job
    infrastructure. Uses PASSES_RECENT (3mo × 0.5mi, 6mo × 1mi,
    12mo × 2mi). Total ~2-3 min. Catches stragglers — recent listings
    (pendings, just-listed actives) the broad 24mo sweep missed due to
    Propelio's 100-cap per call.
    """
    address = str(request.target_address or "").strip()
    if not address:
        raise HTTPException(status_code=400, detail="target_address is required")
    saved_area_id = str(request.saved_area_id or "").strip() or None

    job_id = "rr_" + secrets.token_urlsafe(8)

    conn = get_session_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO propelio_deep_pull_jobs
                    (job_id, target_address, saved_area_id, started_by_user_id, status)
                VALUES (%s, %s, %s, %s, 'queued')
                """,
                (job_id, address, saved_area_id, int(user["id"])),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_session_conn(conn)

    asyncio.create_task(run_deep_pull(job_id, passes=PASSES_RECENT))
    return {"job_id": job_id, "kind": "refresh_recent"}


@router.get("/deep-pull/status/{job_id}")
async def get_deep_pull_status(
    job_id: str,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        raise HTTPException(status_code=400, detail="job_id is required")

    conn = get_session_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE propelio_deep_pull_jobs
                SET status='error',
                    last_error='Worker interrupted (stale)',
                    stop_requested=TRUE
                WHERE job_id = %s
                  AND status='running'
                  AND started_at < NOW() - (%s * INTERVAL '1 minute')
                  AND (last_pass_at IS NULL OR last_pass_at < NOW() - (%s * INTERVAL '1 minute'))
                  AND (next_pass_at IS NULL OR next_pass_at < NOW())
                """,
                (normalized_job_id, STALE_JOB_THRESHOLD_MINUTES, STALE_JOB_THRESHOLD_MINUTES),
            )

            cur.execute(
                """
                SELECT job_id, status, passes_completed, total_unique_comps,
                       net_new_comps,
                       last_pass_at, next_pass_at, last_error, started_at, stop_requested
                FROM propelio_deep_pull_jobs
                WHERE job_id = %s
                """,
                (normalized_job_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Job not found")

            cur.execute(
                """
                SELECT pass_num,
                       months,
                       range_mi,
                       pass_label,
                       COUNT(*) AS returned,
                       COUNT(*) FILTER (WHERE is_first_seen_in_job) AS new_comps,
                       MAX(fetched_at) AS completed_at
                FROM propelio_deep_pull_experiment
                WHERE job_id = %s
                GROUP BY pass_num, months, range_mi, pass_label
                ORDER BY pass_num
                """,
                (normalized_job_id,),
            )
            per_pass = [
                {
                    "pass_num": r[0],
                    "months": r[1],
                    "range_mi": float(r[2]),
                    "label": r[3],
                    "returned": int(r[4] or 0),
                    "new": int(r[5] or 0),
                    "completed_at": r[6].isoformat() if r[6] else None,
                }
                for r in (cur.fetchall() or [])
            ]
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_session_conn(conn)

    return {
        "job_id": row[0],
        "status": row[1],
        "passes_completed": int(row[2] or 0),
        "total_unique_comps": int(row[3] or 0),
        "net_new_comps": int(row[4] or 0),
        "last_pass_at": row[5].isoformat() if row[5] else None,
        "next_pass_at": row[6].isoformat() if row[6] else None,
        "last_error": row[7],
        "started_at": row[8].isoformat() if row[8] else None,
        "stop_requested": bool(row[9]),
        "per_pass": per_pass,
    }


@router.post("/deep-pull/stop/{job_id}")
async def stop_deep_pull(
    job_id: str,
    user: dict = Depends(get_current_user),
) -> dict[str, Any]:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        raise HTTPException(status_code=400, detail="job_id is required")

    conn = get_session_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE propelio_deep_pull_jobs
                SET stop_requested = TRUE
                WHERE job_id = %s
                """,
                (normalized_job_id,),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_session_conn(conn)
    return {"ok": True}


class AttachExistingRequest(BaseModel):
    saved_area_id: str
    comps: list[dict[str, Any]]


@router.post("/attach-to-area")
async def attach_existing_comps(request: AttachExistingRequest) -> dict[str, Any]:
    """Attach an in-memory comp list to an archive (saved area) without
    re-pulling from Propelio. Used right after a user saves a fresh
    polygon: the comps already on screen need stable comp_address_keys
    so the popup rating buttons go live. No cache lookup, no scrape, no
    quota cost — just the merge + reload."""
    saved_area_id = str(request.saved_area_id or "").strip()
    if not saved_area_id:
        raise HTTPException(status_code=400, detail="saved_area_id is required")
    comps = list(request.comps or [])
    archive_meta = merge_comps_into_archive(saved_area_id, comps)
    archived_comps = load_archived_comps(saved_area_id)
    return {"archive_meta": archive_meta, "comps": archived_comps}


class CompRateRequest(BaseModel):
    saved_area_id: str
    comp_address_key: str
    rating: str | None = None


@router.post("/comp/rate")
async def rate_comp(request: CompRateRequest) -> dict[str, Any]:
    """Set or clear a user_rating ('good' | 'bad' | None) on an archived
    comp. Frontend uses this from the comp popup buttons.
    """
    try:
        updated = set_comp_rating(
            saved_area_id=request.saved_area_id,
            comp_address_key=request.comp_address_key,
            rating=request.rating,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if updated == 0:
        raise HTTPException(
            status_code=404,
            detail="No archived comp found for that saved_area_id + comp_address_key",
        )
    return {"ok": True, "rating": request.rating, "updated": updated}
