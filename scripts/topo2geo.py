"""Convert a world-atlas TopoJSON land file into a compact GeoJSON (one-off helper)."""
import json
import sys

src, dst = sys.argv[1], sys.argv[2]
topo = json.load(open(src))
sx, sy = topo["transform"]["scale"]
tx, ty = topo["transform"]["translate"]

arcs = []
for arc in topo["arcs"]:
    x = y = 0
    pts = []
    for dx, dy in arc:
        x += dx
        y += dy
        pts.append([round(x * sx + tx, 2), round(y * sy + ty, 2)])
    arcs.append(pts)


def ring(ids):
    out = []
    for i in ids:
        pts = arcs[i] if i >= 0 else arcs[~i][::-1]
        out.extend(pts if not out else pts[1:])
    return out


feats = []
for geom in topo["objects"]["land"]["geometries"]:
    if geom["type"] == "Polygon":
        polys = [geom["arcs"]]
    else:
        polys = geom["arcs"]
    coords = [[ring(r) for r in p] for p in polys]
    feats.append({"type": "Feature", "properties": {},
                  "geometry": {"type": "MultiPolygon", "coordinates": coords}})

json.dump({"type": "FeatureCollection", "features": feats}, open(dst, "w"), separators=(",", ":"))
print("ok", len(feats))
