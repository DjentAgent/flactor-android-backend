# -*- coding: utf-8 -*-
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import requests

log = logging.getLogger(__name__)
MB_BASE = "https://musicbrainz.org/ws/2"

def _norm(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^\w\s]+", " ", s, flags=re.UNICODE)
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _tokens(s: str) -> List[str]:
    return [t for t in _norm(s).split() if len(t) >= 3]

def _similarity(a: str, b: str) -> float:
    an, bn = _norm(a), _norm(b)
    if not an or not bn:
        return 0.0
    if an == bn:
        return 1.0
    at, bt = set(_tokens(a)), set(_tokens(b))
    if not at or not bt:
        return 0.0
    inter = len(at & bt)
    union = len(at | bt)
    return inter / union if union else 0.0

def _artist_credit_names(ac_list) -> List[str]:
    names = []
    for ac in (ac_list or []):
        nm = ""
        if isinstance(ac, dict):
            nm = (ac.get("name") or "").strip()
            if not nm:
                nm = ((ac.get("artist") or {}).get("name") or "").strip()
        if nm:
            names.append(nm)
    return names

def _artist_matches(query_artist: str, ac_list) -> Tuple[bool, float, bool]:
    """(matched, best_sim, exact_eq) РїРѕ artist-credit."""
    qa = (query_artist or "").strip()
    if not qa:
        return True, 1.0, False
    qa_norm = _norm(qa)
    best_sim = 0.0
    exact = False
    matched = False
    for nm in _artist_credit_names(ac_list):
        sim = _similarity(nm, qa)
        if _norm(nm) == qa_norm:
            exact = True
            matched = True
            best_sim = max(best_sim, 1.0)
        elif sim >= 0.85:
            matched = True
            best_sim = max(best_sim, sim)
    return matched, best_sim, exact

@dataclass
class TrackMeta:
    artist: str
    track: str
    albums: List[str] = field(default_factory=list)
    aliases: List[str] = field(default_factory=list)
    years: List[int] = field(default_factory=list)
    primary_album: Optional[str] = None
    confidence: float = 0.0

_BAD_TOKENS = [
    "live", "remaster", "remastered", "compilation", "best of",
    "tribute", "deluxe", "anniversary", "expanded", "raw", "reissue",
    "promo", "sampler", "dj mix", "mixed"
]

def _is_bad_album_name(name: str) -> bool:
    return any(tok in _norm(name) for tok in _BAD_TOKENS)

def _type_rank(primary_type: str) -> int:
    t = (primary_type or "").lower()
    if t == "album":  return 0
    if t == "ep":     return 1
    if t == "single": return 2
    return 3

def _pick_primary(albums: List[Tuple[str, Optional[int], str, float]]) -> Optional[str]:
    if not albums:
        return None
    def key(it: Tuple[str, Optional[int], str, float]):
        name, year, ptype, weight = it
        return (-weight, _type_rank(ptype), 1 if _is_bad_album_name(name) else 0, (year if year else 9999))
    albums_sorted = sorted(albums, key=key)
    return albums_sorted[0][0] if albums_sorted else None

def resolve_track(artist: str, track: str) -> TrackMeta:
    a_q = (artist or "").strip()
    t_q = (track or "").strip()
    meta = TrackMeta(artist=a_q, track=t_q)
    if not a_q or not t_q:
        return meta

    try:
        params = {
            "query": f'artist:"{a_q}" recording:"{t_q}"',
            "fmt": "json",
            "limit": "50",
            "inc": "releases+release-groups+aliases+artist-credits",
        }
        r = requests.get(f"{MB_BASE}/recording/", params=params, timeout=12)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.debug("MusicBrainz resolve failed: %s", e)
        return meta

    best_albums: List[Tuple[str, Optional[int], str, float]] = []
    aliases_set, years_set = set(), set()
    max_conf = 0.0
    norm_track = _norm(t_q)

    for rec in data.get("recordings", []) or []:
        rec_title = rec.get("title") or ""
        sim = _similarity(rec_title, t_q)
        exact_track = (_norm(rec_title) == norm_track)
        if not exact_track and sim < 0.60:
            continue

        # Р·Р°РїРёСЃСЊ РґРѕР»Р¶РЅР° РїСЂРёРЅР°РґР»РµР¶Р°С‚СЊ РЅР°С€РµРјСѓ Р°СЂС‚РёСЃС‚Сѓ
        rec_match, rec_sim, rec_exact = _artist_matches(a_q, rec.get("artist-credit"))
        if not rec_match:
            continue

        conf_here = (1.0 if exact_track else sim) * (1.0 if rec_exact else max(0.85, rec_sim or 0.85))
        max_conf = max(max_conf, conf_here)

        for al in (rec.get("aliases") or []):
            nm = (al.get("name") or "").strip()
            if nm and _norm(nm) != norm_track:
                aliases_set.add(nm)

        for rel in (rec.get("releases") or []):
            title = (rel.get("title") or "").strip()
            if not title:
                continue

            # artist-credit: release в†’ release-group в†’ recording (fallback)
            rg = rel.get("release-group") or {}
            rel_ac = rel.get("artist-credit") or rg.get("artist-credit") or rec.get("artist-credit") or []
            # РµСЃР»Рё РЅРёС‡РµРіРѕ РЅРµС‚ вЂ” РїСЂРёРјРµРј РєР°Рє matched (РјС‹ СѓР¶Рµ РїСЂРѕРІРµСЂРёР»Рё Р·Р°РїРёСЃСЊ)
            if rel.get("artist-credit") is None and rg.get("artist-credit") is None:
                rel_match, rel_exact = True, False
            else:
                _m, _sim, _exact = _artist_matches(a_q, rel_ac)
                rel_match, rel_exact = _m, _exact
            if not rel_match:
                continue

            # РіРѕРґ
            year = None
            date = rel.get("date") or ""
            if date:
                try: year = int(date[:4])
                except Exception: pass
            if year: years_set.add(year)

            primary_type = (rg.get("primary-type") or "").strip()
            secondary = [(_ or "").lower() for _ in (rg.get("secondary-types") or [])]
            status = (rel.get("status") or "").lower()

            w = max(0.0, sim)
            if exact_track: w = max(w, 0.9)
            if "official" in status: w += 0.10

            p = (primary_type or "").lower()
            if p == "album":   w += 0.25
            elif p == "ep":    w += 0.05
            elif p == "single": w -= 0.25

            if _similarity(title, t_q) >= 0.90:
                w -= 0.20  # single СЃ РЅР°Р·РІР°РЅРёРµРј РєР°Рє Сѓ С‚СЂРµРєР°

            if "live" in secondary:       w -= 0.15
            if p == "compilation":         w -= 0.20
            if _is_bad_album_name(title):  w -= 0.05

            if rel_exact: w += 0.10
            elif rel_match: w += 0.03

            best_albums.append((title, year, primary_type, w))

    # РґРµРґСѓРї РїРѕ РЅРѕСЂРјР°Р»РёР·РѕРІР°РЅРЅРѕРјСѓ РЅР°Р·РІР°РЅРёСЋ
    merged: Dict[str, Tuple[str, Optional[int], str, float]] = {}
    for title, year, ptype, w in best_albums:
        key = _norm(title)
        prev = merged.get(key)
        if not prev or w > prev[3]:
            merged[key] = (title, year, ptype, w)

    albums_list = list(merged.values())
    primary = _pick_primary(albums_list)

    def sort_key(it: Tuple[str, Optional[int], str, float]):
        title, year, ptype, w = it
        return (_type_rank(ptype), 1 if _is_bad_album_name(title) else 0, (year if year else 9999), -w)

    ordered = [x[0] for x in sorted(albums_list, key=sort_key)]

    meta.albums = ordered[:8]
    meta.primary_album = primary
    meta.aliases = list(aliases_set)[:8]
    meta.years = sorted(list(years_set))[:8]
    meta.confidence = float(max_conf if albums_list else 0.0)  # РµСЃР»Рё СЂРµР»РёР·РѕРІ РЅРµ СЃРѕР±СЂР°Р»Рё вЂ” 0.0

    log.debug(
        "MusicBrainz resolved: artist=%r track=%r primary_album=%r albums=%d years=%d aliases=%d confidence=%.2f",
        a_q, t_q, meta.primary_album, len(meta.albums), len(meta.years), len(meta.aliases), meta.confidence
    )
    return meta
