"""uav-fleet: a persistent fleet of SIMULATED copters for WebRobot pipelines.

N ArduCopter SITL instances (ArduPilot Copter-4.7.3, GPLv3) at one base. Each drone gets a MAVProxy that fans its
MAVLink link out to:
  - tcpin:0.0.0.0:<6000+i>  — the Spark task that borrows the drone (stage fleetFly)
  - udp:127.0.0.1:<7000+i>  — this process, which keeps the live state of the fleet
and an HTTP server on :8080 serves a live map (/) and the fleet state (/api/fleet).
Configuration by environment: FLEET_N, FLEET_LAT, FLEET_LON, FLEET_SPEEDUP, SITL_BASE (download URL)."""
import asyncio, json, math, os, socket, subprocess, sys, tempfile, time, urllib.request, hashlib
from aiohttp import web
from pymavlink import mavutil

N = int(os.environ.get("FLEET_N", "5"))
LAT = float(os.environ.get("FLEET_LAT", "44.2850"))
LON = float(os.environ.get("FLEET_LON", "11.8830"))
SPEEDUP = int(os.environ.get("FLEET_SPEEDUP", "10"))
BASE = os.environ.get("SITL_BASE", "https://s3.metaglobe.finance/sparklogs-web/tools/ardupilot/copter-4.7.3/")
EARTH = 6378137.0
PLAN = {"points": [], "acquired": [], "mission": "", "updated": 0.0}
LEASES = {}   # drone id -> {"holder": str, "until": epoch}: a Spark task borrows a drone, as spookystuff's Dispatcher does
STATE = {i: {"id": i, "lat": None, "lon": None, "alt": 0.0, "hdg": 0, "speed": 0.0, "battery": None, "mode": "", "armed": False,
             "trail": [], "updated": 0.0, "spark_port": 6000 + i} for i in range(N)}
EXTRA_PARAMS = "BATT_CAPACITY 20000\nSIM_BATT_CAP_AH 20\nWPNAV_SPEED 1200\nRTL_ALT 2500\n"   # long-endurance pack for survey work


def offset(lat, lon, dn, de):
    return lat + math.degrees(dn / EARTH), lon + math.degrees(de / (EARTH * math.cos(math.radians(lat))))


def install():
    d = os.environ.get("SITL_DIR", "/opt/sitl"); os.makedirs(d, exist_ok=True)
    want = urllib.request.urlopen(BASE + "arducopter.sha256", timeout=60).read().decode().strip()[:64]
    b = os.path.join(d, "arducopter")
    if not (os.path.exists(b) and hashlib.sha256(open(b, "rb").read()).hexdigest() == want):
        data = urllib.request.urlopen(BASE + "arducopter", timeout=300).read()
        assert hashlib.sha256(data).hexdigest() == want, "simulator checksum mismatch"
        open(b, "wb").write(data); os.chmod(b, 0o755)
    p = os.path.join(d, "copter.parm")
    open(p, "wb").write(urllib.request.urlopen(BASE + "copter.parm", timeout=60).read())
    x = os.path.join(d, "fleet.parm"); open(x, "w").write(EXTRA_PARAMS)
    return b, p, x


def start_drones(binary, parm, extra):
    procs = []
    for i in range(N):
        home = offset(LAT, LON, 0, (i - (N - 1) / 2) * 8)          # pads 8 m apart on an east-west line
        wd = tempfile.mkdtemp(prefix=f"drone{i}_")
        procs.append(subprocess.Popen([binary, "--model", "+", "--speedup", str(SPEEDUP), "-I", str(i),
                                       "--home", f"{home[0]:.7f},{home[1]:.7f},0,0", "--defaults", f"{parm},{extra}"],
                                      cwd=wd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        STATE[i]["lat"], STATE[i]["lon"] = home
    time.sleep(3)
    for i in range(N):
        procs.append(subprocess.Popen(["mavproxy.py", f"--master=tcp:127.0.0.1:{5760 + 10 * i}", f"--out=tcpin:0.0.0.0:{6000 + i}",
                                       f"--out=udp:127.0.0.1:{7000 + i}", "--daemon", "--non-interactive", f"--state-basedir=/tmp/mavproxy{i}"],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    return procs


def watch(i):
    """Live state of drone i from its MAVProxy UDP out."""
    m = mavutil.mavlink_connection(f"udpin:127.0.0.1:{7000 + i}", source_system=240 + i)
    while True:
        msg = m.recv_match(type=["GLOBAL_POSITION_INT", "SYS_STATUS", "VFR_HUD", "HEARTBEAT"], blocking=True, timeout=5)
        if msg is None: continue
        s, t = STATE[i], msg.get_type()
        if t == "GLOBAL_POSITION_INT":
            s["lat"], s["lon"], s["alt"], s["hdg"] = msg.lat / 1e7, msg.lon / 1e7, round(msg.relative_alt / 1000, 1), msg.hdg / 100
            if s["alt"] > 1 and (not s["trail"] or abs(s["trail"][-1][0] - s["lat"]) + abs(s["trail"][-1][1] - s["lon"]) > 2e-5):
                s["trail"] = (s["trail"] + [[s["lat"], s["lon"]]])[-600:]
            if s["alt"] < 0.5 and not s["armed"] and len(s["trail"]) > 0 and time.time() - s["updated"] > 30: s["trail"] = []
        elif t == "SYS_STATUS": s["battery"] = msg.battery_remaining
        elif t == "VFR_HUD": s["speed"] = round(msg.groundspeed, 1)
        elif t == "HEARTBEAT" and msg.type == 2:
            s["armed"] = bool(msg.base_mode & 128); s["mode"] = mavutil.mode_string_v10(msg)
        s["updated"] = time.time()


async def api_plan(req):
    """The planner publishes the mission: points with the drone and the order assigned to each (shown on the map)."""
    b = await req.json()
    PLAN.update(points=b.get("points", [])[:2000], mission=str(b.get("mission", ""))[:120], acquired=[], updated=time.time())
    return web.json_response({"ok": True, "points": len(PLAN["points"])})


async def api_acquired(req):
    b = await req.json()
    PLAN["acquired"] = (PLAN["acquired"] + [b])[-5000:]
    return web.json_response({"ok": True})


async def api_lease(req):
    """POST {drone, holder, ttl_s}: grants the drone if free (or already held by the same holder) — 200 or 409."""
    b = await req.json(); d = int(b.get("drone", -1)); holder = str(b.get("holder", ""))[:120]; ttl = min(7200, max(30, int(b.get("ttl_s", 1800))))
    if d not in STATE or not holder: return web.json_response({"ok": False, "error": "bad request"}, status=400)
    cur = LEASES.get(d)
    if cur and cur["until"] > time.time() and cur["holder"] != holder:
        return web.json_response({"ok": False, "holder": cur["holder"], "until": cur["until"]}, status=409)
    LEASES[d] = {"holder": holder, "until": time.time() + ttl}
    return web.json_response({"ok": True, "drone": d, "port": 6000 + d, "until": LEASES[d]["until"]})


async def api_release(req):
    b = await req.json(); d = int(b.get("drone", -1))
    if LEASES.get(d, {}).get("holder") == str(b.get("holder", "")): LEASES.pop(d, None)
    return web.json_response({"ok": True})


async def api_fleet(_):
    return web.json_response({"base": [LAT, LON], "speedup": SPEEDUP, "drones": [dict(x, leased_by=(LEASES.get(x["id"]) or {}).get("holder") if (LEASES.get(x["id"]) or {}).get("until", 0) > time.time() else None) for x in STATE.values()], "plan": PLAN,
                              "source": "ArduPilot SITL Copter-4.7.3 (simulated), MAVProxy per drone"})


async def index(_):
    return web.Response(text=open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "map.html")).read(), content_type="text/html")


def main():
    binary, parm, extra = install()
    procs = start_drones(binary, parm, extra)
    loop = asyncio.new_event_loop()
    for i in range(N): loop.run_in_executor(None, watch, i)
    app = web.Application(); app.router.add_get("/", index); app.router.add_get("/api/fleet", api_fleet)
    app.router.add_post("/api/plan", api_plan); app.router.add_post("/api/acquired", api_acquired)
    app.router.add_post("/api/lease", api_lease); app.router.add_post("/api/release", api_release)
    app.router.add_get("/healthz", lambda _: web.Response(text="ok"))
    try:
        web.run_app(app, host="0.0.0.0", port=8080, loop=loop, print=None)
    finally:
        for p in procs: p.terminate()


if __name__ == "__main__":
    main()
