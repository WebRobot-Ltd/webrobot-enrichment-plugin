"""sumo-sim: a light urban traffic SIMULATOR service (Eclipse SUMO, headless) for impact studies.

Spark stages (trafficNetwork / trafficSimulate) acquire from it:
  POST /api/network {name, south, west, north, east}          -> builds the road network from OpenStreetMap
                                                                 and returns one record per road edge
  POST /api/run {network, label, closed_edges[], vehicles_per_hour, duration_s, seed}
                                                              -> runs one scenario (same random OD demand for
                                                                 the same network/vph/duration/seed) and returns KPIs
  GET  /api/runs, GET /healthz

Stdlib only (plus the eclipse-sumo wheel). One simulation at a time (SUMO_MAX_PARALLEL), queued, so the pod stays
small. Simulated traffic from random origin-destination demand: an impact SCREENING, not measured counts.
"""
import html
import json
import math
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import re

import sumo
import sumolib

SUMO_HOME = os.environ.setdefault("SUMO_HOME", os.path.dirname(sumo.__file__))
BIN = Path(SUMO_HOME) / "bin"
TOOLS = Path(SUMO_HOME) / "tools"
DATA = Path(os.environ.get("SUMO_DATA", "/data")).resolve()
MAX_KM2 = float(os.environ.get("SUMO_MAX_KM2", "16"))
MAX_VPH = 6000
MAX_DURATION = 7200
OVERPASS = os.environ.get("OVERPASS_URL", "https://overpass-api.de/api/interpreter")
ROADS = "motorway|trunk|primary|secondary|tertiary|unclassified|residential|living_street"
SIM_SLOTS = threading.Semaphore(int(os.environ.get("SUMO_MAX_PARALLEL", "1")))
NET_LOCK = threading.Lock()
SOURCE = "Eclipse SUMO 1.28 simulation; road network OpenStreetMap contributors (ODbL)"


def street(edge):
    return html.unescape(edge.getName() or "")


def safe(name):
    s = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(name).strip().lower()).strip("-")
    if not s:
        raise ValueError("invalid name")
    return s[:40]


def run(cmd, timeout=1800, cwd=None):
    p = subprocess.run([str(c) for c in cmd], capture_output=True, text=True, timeout=timeout, cwd=cwd)
    if p.returncode != 0:
        raise RuntimeError(f"{Path(str(cmd[0])).name} failed: {(p.stderr or p.stdout)[-1200:]}")


def area_km2(s, w, n, e):
    return abs((n - s) * 111.32 * (e - w) * 111.32 * math.cos(math.radians((n + s) / 2)))


_nets = {}


def load_net(path):
    key = (str(path), path.stat().st_mtime)
    if key not in _nets:
        _nets.clear()
        _nets[key] = sumolib.net.readNet(str(path), withInternal=False)
    return _nets[key]


# ------------------------------------------------------------------ network
REGION_LOCK = threading.Lock()


def region_roads():
    """The regional OpenStreetMap extract (Geofabrik, ODbL), downloaded ONCE and reduced to roads only, so every area
    is then cut locally in seconds: no per-request load on shared public services."""
    url = os.environ.get("OSM_PBF_URL", "")
    if not url or not shutil.which("osmium"):
        return None
    d = DATA / "osm"
    roads = d / "region-roads.osm.pbf"
    with REGION_LOCK:
        if not roads.exists():
            d.mkdir(parents=True, exist_ok=True)
            part = d / "region.osm.pbf.part"
            req = urllib.request.Request(url, headers={"User-Agent": "webrobot-sumo-sim/0.1"})
            with urllib.request.urlopen(req, timeout=600) as resp, open(part, "wb") as f:
                shutil.copyfileobj(resp, f, 1 << 20)
            run(["osmium", "tags-filter", part, f"w/highway={ROADS.replace('|', ',')}",
                 *[f"w/highway={r}_link" for r in ("motorway", "trunk", "primary", "secondary", "tertiary")],
                 "-o", roads, "--overwrite"], timeout=3600)
            part.unlink(missing_ok=True)
    return roads


def fetch_osm(query, dest, bbox):
    s, w, n, e = bbox
    roads = region_roads()
    if roads is not None:
        run(["osmium", "extract", "-b", f"{w},{s},{e},{n}", "--strategy", "complete_ways", roads,
             "-o", dest, "-f", "osm", "--overwrite"], timeout=600)
        if dest.stat().st_size > 1000:
            return
        raise ValueError("area outside the regional OpenStreetMap extract (OSM_PBF_URL)")
    # fallback: Overpass, a shared public service — a few spaced retries, then the next mirror
    last = None
    for url in [u for u in OVERPASS.split(",") if u] + ["https://overpass.private.coffee/api/interpreter"]:
        for attempt in range(3):
            try:
                r = urllib.request.Request(url, data=urllib.parse.urlencode({"data": query}).encode(),
                                           headers={"User-Agent": "webrobot-sumo-sim/0.1"})
                with urllib.request.urlopen(r, timeout=180) as resp, open(dest, "wb") as f:
                    shutil.copyfileobj(resp, f)
                if dest.stat().st_size > 1000:
                    return
                last = RuntimeError("empty OSM answer")
            except Exception as e:  # noqa: BLE001
                last = e
            time.sleep(10 * (attempt + 1))
    raise RuntimeError(f"OpenStreetMap download failed: {last}")


def build_network(req):
    name = safe(req["name"])
    s, w, n, e = (float(req[k]) for k in ("south", "west", "north", "east"))
    if not (s < n and w < e):
        raise ValueError("bbox must satisfy south<north and west<east")
    km2 = area_km2(s, w, n, e)
    if km2 > MAX_KM2:
        raise ValueError(f"area {km2:.1f} km2 exceeds limit {MAX_KM2} km2")
    d = DATA / "networks" / name
    meta_f = d / "meta.json"
    with NET_LOCK:
        meta = json.loads(meta_f.read_text()) if meta_f.exists() else None
        if not meta or meta.get("bbox") != [s, w, n, e] or req.get("rebuild"):
            d.mkdir(parents=True, exist_ok=True)
            q = (f'[out:xml][timeout:120];(way["highway"~"^({ROADS})(_link)?$"]({s},{w},{n},{e}););(._;>;);out body;')
            fetch_osm(q, d / "map.osm", (s, w, n, e))
            run([BIN / "netconvert", "--osm-files", d / "map.osm", "-o", d / "net.net.xml",
                 "--geometry.remove", "--ramps.guess", "--junctions.join", "--tls.guess-signals",
                 "--tls.discard-simple", "--tls.join", "--keep-edges.by-vclass", "passenger",
                 "--remove-edges.isolated", "--output.street-names", "--no-warnings"])
            (d / "map.osm").unlink(missing_ok=True)
            shutil.rmtree(d / "demand", ignore_errors=True)
            net = load_net(d / "net.net.xml")
            meta = {"network": name, "bbox": [s, w, n, e], "area_km2": round(km2, 2),
                    "edges": len(net.getEdges()), "junctions": len(net.getNodes()),
                    "traffic_lights": len(net.getTrafficLights()),
                    "road_km": round(sum(x.getLength() for x in net.getEdges()) / 1000, 1),
                    "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
            meta_f.write_text(json.dumps(meta))
    net = load_net(d / "net.net.xml")
    edges = []
    for x in net.getEdges():
        shape = x.getShape()
        mx, my = shape[len(shape) // 2] if len(shape) > 2 else ((shape[0][0] + shape[-1][0]) / 2, (shape[0][1] + shape[-1][1]) / 2)
        lon, lat = net.convertXY2LonLat(mx, my)
        edges.append({"edge_id": x.getID(), "street": street(x), "road_type": x.getType().replace("highway.", ""),
                      "lat": round(lat, 6), "lon": round(lon, 6), "length_m": round(x.getLength(), 1),
                      "speed_kmh": round(x.getSpeed() * 3.6), "lanes": x.getLaneNumber(),
                      "from_node": x.getFromNode().getID(), "to_node": x.getToNode().getID()})
    return {**meta, "source": SOURCE, "edge_list": edges}


# ------------------------------------------------------------------- demand
def demand(netdir, vph, duration, seed):
    key = f"v{vph}-d{duration}-s{seed}"
    dd = netdir / "demand"
    trips = dd / f"{key}.trips.xml"
    with NET_LOCK:
        if not trips.exists():
            tmp = dd / f"{key}.{uuid.uuid4().hex[:6]}"
            tmp.mkdir(parents=True)
            run([sys.executable, TOOLS / "randomTrips.py", "-n", netdir / "net.net.xml", "-o", tmp / "trips.xml",
                 "-r", tmp / "routes.rou.xml", "-b", "0", "-e", str(duration), "-p", f"{3600 / vph:.4f}",
                 "--fringe-factor", "5", "--validate", "--seed", str(seed),
                 "--trip-attributes", 'departLane="best" departSpeed="max"'], cwd=tmp)
            shutil.move(tmp / "trips.xml", trips)
            shutil.rmtree(tmp, ignore_errors=True)
    n = sum(1 for _ in ET.parse(trips).getroot().iter("trip"))
    return trips, key, n


# --------------------------------------------------------------- simulation
def stats(rd, net):
    ti = ET.parse(rd / "tripinfo.xml").getroot()
    trips = list(ti.iter("tripinfo"))
    n = len(trips)
    avg = (lambda a: round(sum(float(t.get(a, 0)) for t in trips) / n, 1) if n else None)
    st = ET.parse(rd / "stats.xml").getroot()
    veh, tel = st.find("vehicles"), st.find("teleports")
    agg = {}
    for ed in ET.parse(rd / "edgedata.xml").getroot().iter("edge"):
        try:
            nm = street(net.getEdge(ed.get("id"))) or ed.get("id")
        except KeyError:
            continue
        a = agg.setdefault(nm, [0.0, 0.0, 0])
        a[0] += float(ed.get("waitingTime", 0)); a[1] += float(ed.get("timeLoss", 0)); a[2] += int(float(ed.get("entered", 0)))
    top = sorted(agg.items(), key=lambda kv: -kv[1][0])[:10]
    co2 = sum(float(e.get("CO2_abs", 0)) for e in ti.iter("emissions"))
    return {"vehicles_loaded": int(veh.get("loaded")), "vehicles_inserted": int(veh.get("inserted")),
            "trips_completed": n, "still_running_at_end": int(veh.get("running")),
            "teleports": int(tel.get("total")) if tel is not None else 0,
            "avg_trip_duration_s": avg("duration"), "avg_waiting_s": avg("waitingTime"),
            "avg_time_loss_s": avg("timeLoss"), "avg_route_length_m": avg("routeLength"),
            "total_time_loss_h": round(sum(float(t.get("timeLoss", 0)) for t in trips) / 3600, 1),
            "total_co2_kg": round(co2 / 1e6, 1),
            "hotspots": [{"street": k, "waiting_s": round(v[0]), "time_loss_s": round(v[1]), "vehicles": v[2]} for k, v in top]}


def run_scenario(req):
    netdir = DATA / "networks" / safe(req["network"])
    if not (netdir / "net.net.xml").exists():
        raise ValueError(f"network '{req['network']}' not built: call /api/network first")
    vph = int(req.get("vehicles_per_hour", 2000)); dur = int(req.get("duration_s", 1800)); seed = int(req.get("seed", 42))
    if not (1 <= vph <= MAX_VPH and 60 <= dur <= MAX_DURATION):
        raise ValueError(f"vehicles_per_hour 1..{MAX_VPH}, duration_s 60..{MAX_DURATION}")
    trips, dkey, ntrips = demand(netdir, vph, dur, seed)
    net = load_net(netdir / "net.net.xml")
    closed = sorted({e for e in req.get("closed_edges", []) or [] if net.hasEdge(e)})
    rid = time.strftime("%m%d-%H%M%S-") + uuid.uuid4().hex[:4]
    rd = DATA / "runs" / rid
    rd.mkdir(parents=True)
    net_file = netdir / "net.net.xml"
    t0 = time.time()
    with SIM_SLOTS:
        queued = round(time.time() - t0, 1)
        if closed:
            (rd / "closed.txt").write_text("\n".join(closed))
            net_file = rd / "scenario.net.xml"
            run([BIN / "netconvert", "-s", netdir / "net.net.xml", "--remove-edges.input-file", rd / "closed.txt",
                 "-o", net_file, "--no-warnings"])
            # trips that start or end on a closed road cannot happen at all: drop them (counted as trips_impossible)
            cs = set(closed)
            tree = ET.parse(trips); root = tree.getroot()
            for t in list(root.iter("trip")):
                if t.get("from") in cs or t.get("to") in cs or cs.intersection((t.get("via") or "").split()):
                    root.remove(t)
            trips = rd / "trips.xml"
            tree.write(trips)
        (rd / "ed.add.xml").write_text('<additional><edgeData id="ed" file="edgedata.xml" excludeEmpty="true"/></additional>')
        end = dur + 1200
        t1 = time.time()
        run([BIN / "sumo", "-n", net_file, "-r", trips, "-a", "ed.add.xml", "--end", str(end),
             "--ignore-route-errors", "--no-step-log", "--no-warnings",
             "--tripinfo-output", "tripinfo.xml", "--statistic-output", "stats.xml",
             "--device.emissions.probability", "1", "--device.rerouting.probability", "1",
             "--device.rerouting.period", "60", "--time-to-teleport", "300", "--seed", "42"], cwd=rd)
        wall = round(time.time() - t1, 1)
    res = {"run_id": rid, "label": str(req.get("label", "baseline"))[:80], "network": netdir.name, "demand": dkey,
           "trips_planned": ntrips, "closed_edges": len(closed),
           "closed_streets": sorted({street(net.getEdge(e)) for e in closed} - {""}),
           "closed_road_km": round(sum(net.getEdge(e).getLength() for e in closed) / 1000, 2),
           "simulated_s": end, "queued_s": queued, "wall_clock_s": wall, **stats(rd, net), "source": SOURCE}
    res["trips_impossible"] = max(0, ntrips - res["vehicles_loaded"])   # origin or destination cut off by closures
    for f in ("scenario.net.xml", "tripinfo.xml", "edgedata.xml", "trips.xml"):
        (rd / f).unlink(missing_ok=True)
    (rd / "result.json").write_text(json.dumps(res))
    return res


def list_runs():
    out = []
    for p in sorted((DATA / "runs").glob("*/result.json"), reverse=True)[:100]:
        r = json.loads(p.read_text()); r.pop("hotspots", None); r.pop("closed_streets", None); out.append(r)
    return out


# --------------------------------------------------------------------- http
class H(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)

    def do_GET(self):
        if self.path == "/healthz":
            return self._send(200, {"ok": True})
        if self.path.startswith("/api/runs"):
            return self._send(200, list_runs())
        if self.path.startswith("/api/networks"):
            return self._send(200, [json.loads(p.read_text()) for p in sorted((DATA / "networks").glob("*/meta.json"))])
        self._send(404, {"error": "not found"})

    def do_POST(self):
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)) or 0) or b"{}")
            if self.path == "/api/network":
                return self._send(200, build_network(body))
            if self.path == "/api/run":
                return self._send(200, run_scenario(body))
            self._send(404, {"error": "not found"})
        except (ValueError, KeyError) as e:
            self._send(400, {"error": str(e)})
        except Exception as e:  # noqa: BLE001
            self._send(500, {"error": f"{type(e).__name__}: {e}"[:1500]})

    def log_message(self, fmt, *a):
        sys.stderr.write("%s %s\n" % (time.strftime("%H:%M:%S"), fmt % a))


if __name__ == "__main__":
    (DATA / "networks").mkdir(parents=True, exist_ok=True)
    (DATA / "runs").mkdir(parents=True, exist_ok=True)
    port = int(os.environ.get("PORT", "8080"))
    print(f"sumo-sim on :{port}, data {DATA}, SUMO {BIN}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), H).serve_forever()
