"""
AIS collector for the "Мой флот" site.

Runs inside GitHub Actions every ~30 minutes:
  1. downloads the current database from the live site (so no data is committed to git),
  2. listens to the free aisstream.io stream for LISTEN_SECONDS,
  3. merges ship identity (name, IMO, call sign, type, size) and last positions,
  4. writes site/data/ships.json and site/data/meta.json for deployment.

Environment:
  AISSTREAM_KEY   API key from aisstream.io (GitHub secret). Without it the script only
                  republishes the previous data.
  SITE_URL        Public URL of the site, e.g. https://kirov073-lang.github.io/ship-tracker
  LISTEN_SECONDS  How long to listen (default 270).
  AIS_URL         Override of the stream URL (used by tests).
"""
import asyncio
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

import websockets

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "site", "data")
AIS_URL = os.environ.get("AIS_URL", "wss://stream.aisstream.io/v0/stream")
KEY = os.environ.get("AISSTREAM_KEY", "").strip()
SITE_URL = os.environ.get("SITE_URL", "").rstrip("/")
LISTEN = int(os.environ.get("LISTEN_SECONDS", "270"))

# Column order of every ship row in ships.json. The site reads the same list.
FIELDS = ["mmsi", "imo", "name", "call", "type", "len", "beam", "draught", "dest", "eta",
          "lat", "lon", "sog", "cog", "hdg", "nav", "tpos", "tstatic", "prev"]
IDX = {f: i for i, f in enumerate(FIELDS)}


def now():
    return int(time.time())


def clean(s):
    """AIS text is padded with spaces and '@'."""
    if not isinstance(s, str):
        return ""
    return " ".join(s.replace("@", " ").split()).upper()


def load_previous():
    """Fetch the database that is live on the site right now."""
    if not SITE_URL:
        local = os.path.join(OUT_DIR, "ships.json")
        if os.path.exists(local):
            with open(local) as fh:
                return json.load(fh)
        return None
    url = f"{SITE_URL}/data/ships.json?t={now()}"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return json.load(r)
    except Exception as e:  # first run: nothing deployed yet
        print(f"no previous data ({e})")
        return None


def to_index(doc):
    ships = {}
    if not doc:
        return ships
    fields = doc.get("fields", FIELDS)
    for row in doc.get("ships", []):
        rec = dict(zip(fields, row))
        full = [rec.get(f) for f in FIELDS]
        if full[IDX["prev"]] is None:
            full[IDX["prev"]] = ""
        ships[int(full[IDX["mmsi"]])] = full
    return ships


def blank(mmsi):
    row = [None] * len(FIELDS)
    row[IDX["mmsi"]] = mmsi
    row[IDX["prev"]] = ""
    return row


def apply_static(ships, mmsi, msg, ts):
    imo = int(msg.get("ImoNumber") or 0)
    typ = int(msg.get("Type") or 0)
    name = clean(msg.get("Name"))
    known = mmsi in ships
    # Keep merchant/passenger ships: those with an IMO number or a cargo/tanker/passenger type.
    if not known and imo <= 0 and not (60 <= typ <= 89):
        return False
    row = ships.get(mmsi) or blank(mmsi)
    old = row[IDX["name"]]
    if old and name and old != name:
        prev = [p for p in (row[IDX["prev"]] or "").split("|") if p]
        if old not in prev:
            prev.append(old)
        row[IDX["prev"]] = "|".join(prev[-6:])
    if name:
        row[IDX["name"]] = name
    if imo > 0:
        row[IDX["imo"]] = imo
    call = clean(msg.get("CallSign"))
    if call:
        row[IDX["call"]] = call
    if typ:
        row[IDX["type"]] = typ
    d = msg.get("Dimension") or {}
    length = int(d.get("A") or 0) + int(d.get("B") or 0)
    beam = int(d.get("C") or 0) + int(d.get("D") or 0)
    if length:
        row[IDX["len"]] = length
    if beam:
        row[IDX["beam"]] = beam
    dr = msg.get("MaximumStaticDraught")
    if dr:
        row[IDX["draught"]] = round(float(dr), 1)
    dest = clean(msg.get("Destination"))
    if dest:
        row[IDX["dest"]] = dest
    eta = msg.get("Eta") or {}
    if eta.get("Month") and eta.get("Day"):
        row[IDX["eta"]] = "%02d-%02d %02d:%02d" % (int(eta["Month"]), int(eta["Day"]),
                                                    int(eta.get("Hour") or 0) % 24,
                                                    int(eta.get("Minute") or 0) % 60)
    row[IDX["tstatic"]] = ts
    ships[mmsi] = row
    return True


def apply_position(pending, mmsi, msg, meta_name, ts):
    lat, lon = msg.get("Latitude"), msg.get("Longitude")
    if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180:
        return
    if lat == 0 and lon == 0:
        return
    hdg = msg.get("TrueHeading")
    pending[mmsi] = {
        "lat": round(float(lat), 4), "lon": round(float(lon), 4),
        "sog": round(float(msg.get("Sog") or 0), 1) if (msg.get("Sog") or 0) < 102.3 else None,
        "cog": round(float(msg.get("Cog") or 0)) if (msg.get("Cog") or 0) < 360 else None,
        "hdg": int(hdg) if hdg is not None and hdg < 360 else None,
        "nav": msg.get("NavigationalStatus"),
        "tpos": ts, "name": clean(meta_name),
    }


async def listen(ships, pending, stats):
    sub = {
        "APIKey": KEY,
        "BoundingBoxes": [[[-90, -180], [90, 180]]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }
    deadline = time.monotonic() + LISTEN
    while time.monotonic() < deadline - 10:
        stats["connects"] = stats.get("connects", 0) + 1
        try:
            async with websockets.connect(AIS_URL, max_size=2**22, open_timeout=20,
                                          ping_interval=20) as ws:
                await ws.send(json.dumps(sub))
                while True:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        return
                    raw = await asyncio.wait_for(ws.recv(), timeout=min(left, 60))
                    m = json.loads(raw)
                    if "error" in m:
                        print("stream error:", m["error"])
                        stats["errors"] += 1
                        stats["last_error"] = str(m["error"])[:200]
                        if "key" in str(m["error"]).lower():
                            return  # wrong key: retrying will not help
                        break
                    kind = m.get("MessageType")
                    body = (m.get("Message") or {}).get(kind) or {}
                    meta = m.get("MetaData") or {}
                    mmsi = int(body.get("UserID") or meta.get("MMSI") or 0)
                    if not (100000000 <= mmsi <= 999999999):
                        continue
                    ts = now()
                    stats["messages"] += 1
                    if kind == "ShipStaticData":
                        if apply_static(ships, mmsi, body, ts):
                            stats["static"] += 1
                    elif kind == "PositionReport":
                        apply_position(pending, mmsi, body, meta.get("ShipName"), ts)
        except asyncio.TimeoutError:
            if time.monotonic() >= deadline:
                return
            print("stream quiet, reconnecting")
        except Exception as e:
            stats["errors"] += 1
            stats["last_error"] = repr(e)[:200]
            print(f"connection problem: {e!r}")
        await asyncio.sleep(10)


def merge_positions(ships, pending):
    n = 0
    for mmsi, p in pending.items():
        row = ships.get(mmsi)
        if row is None:
            continue  # identity unknown yet; it arrives with the ship's static message
        for k in ("lat", "lon", "sog", "cog", "hdg", "nav", "tpos"):
            row[IDX[k]] = p[k]
        if not row[IDX["name"]] and p["name"]:
            row[IDX["name"]] = p["name"]
        n += 1
    return n


def write(ships, stats, prev_meta):
    os.makedirs(OUT_DIR, exist_ok=True)
    rows = sorted(ships.values(), key=lambda r: (r[IDX["name"]] or "~", r[IDX["mmsi"]]))
    stamp = now()
    doc = {"fields": FIELDS, "updated": stamp, "ships": rows}
    with open(os.path.join(OUT_DIR, "ships.json"), "w") as fh:
        json.dump(doc, fh, separators=(",", ":"), ensure_ascii=False)
    meta = {
        "updated": stamp,
        "updated_iso": datetime.fromtimestamp(stamp, timezone.utc).isoformat(),
        "ships": len(rows),
        "with_position": sum(1 for r in rows if r[IDX["lat"]] is not None),
        "last_run": stats,
        "runs": int((prev_meta or {}).get("runs", 0)) + (1 if stats.get("listened") else 0),
    }
    with open(os.path.join(OUT_DIR, "meta.json"), "w") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=1)
    print(json.dumps(meta, ensure_ascii=False))


def load_prev_meta():
    if not SITE_URL:
        return None
    try:
        with urllib.request.urlopen(f"{SITE_URL}/data/meta.json?t={now()}", timeout=20) as r:
            return json.load(r)
    except Exception:
        return None


def main():
    prev = load_previous()
    ships = to_index(prev)
    print(f"loaded {len(ships)} ships")
    stats = {"messages": 0, "static": 0, "positions": 0, "errors": 0, "listened": False}
    if KEY:
        pending = {}
        asyncio.run(listen(ships, pending, stats))
        stats["positions"] = merge_positions(ships, pending)
        stats["listened"] = True
    else:
        print("AISSTREAM_KEY is not set: publishing previous data only")
    write(ships, stats, load_prev_meta())
    return 0


if __name__ == "__main__":
    sys.exit(main())
