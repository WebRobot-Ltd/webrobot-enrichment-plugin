"""Prepare a HubSpot portal for the WebRobot GTM pipeline (social selling). Idempotent: run it again and it only
creates what is missing.

  HUBSPOT_TOKEN=pat-... python3 setup_hubspot.py

Creates, on companies / contacts / deals, a "WebRobot" property group and the properties the hubspotSync stage
writes (wr_*). Upsert keys: companies.wr_key (normalised domain, unique) and deals.wr_signal_id (unique), contacts
use the native email. Also creates the deal pipeline "Social selling" (Signal → Qualified → Contacted → Replied →
Demo → Proposal → Won / Lost). Needs a private app token with crm.objects.* and crm.schemas.* read+write.
"""
import json
import os
import sys
import urllib.error
import urllib.request

API = "https://api.hubapi.com"
TOKEN = os.environ.get("HUBSPOT_TOKEN", "")
GROUP = {"name": "webrobot", "label": "WebRobot", "displayOrder": -1}
SIGNAL_TYPES = [("tender", "Public tender"), ("hiring", "Hiring"), ("funding", "Funding / news"),
                ("registry", "Company registry"), ("conversation", "Public conversation"),
                ("engagement", "Engagement with our content"), ("inbound", "Inbound request"), ("other", "Other")]


def prop(name, label, type_="string", field="text", unique=False, options=None, desc=""):
    p = {"name": name, "label": label, "type": type_, "fieldType": field, "groupName": GROUP["name"],
         "description": desc}
    if unique:
        p["hasUniqueValue"] = True
    if options:
        p["options"] = [{"label": l, "value": v, "displayOrder": i} for i, (v, l) in enumerate(options)]
    return p


COMMON = [
    prop("wr_icp_profile", "ICP profile", desc="Ideal-customer profile that matched (configurable in the GTM app)"),
    prop("wr_icp_score", "ICP score", "number", "number", desc="0-100: ICP fit × signal strength × freshness"),
    prop("wr_source", "WebRobot source", desc="Pipeline / dataset the record came from"),
    prop("wr_icp_reason", "ICP reason", "string", "textarea", desc="Why the signal fits the ICP (icpScore)"),
]
PROPS = {
    "companies": COMMON + [
        prop("wr_key", "WebRobot key", unique=True, desc="Normalised domain (or registry id): upsert key"),
        prop("wr_last_signal_type", "Last signal type", "enumeration", "select", options=SIGNAL_TYPES),
        prop("wr_last_signal_url", "Last signal URL"),
        prop("wr_last_signal_at", "Last signal date", "date", "date"),
    ],
    "contacts": COMMON + [prop("wr_signal_url", "Signal URL")],
    "deals": COMMON + [
        prop("wr_signal_id", "Signal id", unique=True, desc="Stable id of the signal: upsert key"),
        prop("wr_signal_type", "Signal type", "enumeration", "select", options=SIGNAL_TYPES),
        prop("wr_signal_url", "Signal URL"),
        prop("wr_signal_date", "Signal date", "date", "date"),
        prop("wr_signal_summary", "Signal summary", "string", "textarea"),
    ],
}
STAGES = [("Signal", 0.05), ("Qualified", 0.1), ("Contacted", 0.2), ("Replied", 0.35), ("Demo", 0.5),
          ("Proposal", 0.7), ("Won", 1.0), ("Lost", 0.0)]


def call(method, path, body=None):
    req = urllib.request.Request(API + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Bearer " + TOKEN, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def main():
    if not TOKEN:
        sys.exit("HUBSPOT_TOKEN missing")
    for obj, props in PROPS.items():
        code, _ = call("POST", f"/crm/v3/properties/{obj}/groups", GROUP)
        print(f"{obj}: group", "created" if code == 201 else ("exists" if code == 409 else f"HTTP {code}"))
        code, cur = call("GET", f"/crm/v3/properties/{obj}")
        have = {p["name"] for p in cur.get("results", [])}
        for p in props:
            if p["name"] in have:
                print(f"  {p['name']}: exists")
                continue
            code, r = call("POST", f"/crm/v3/properties/{obj}", p)
            print(f"  {p['name']}:", "created" if code == 201 else f"HTTP {code} {r.get('message', '')[:160]}")
    code, cur = call("GET", "/crm/v3/pipelines/deals")
    pipe = next((p for p in cur.get("results", []) if p["label"] == "Social selling"), None)
    if pipe:
        print("pipeline Social selling: exists", pipe["id"])
    else:
        body = {"label": "Social selling", "displayOrder": 1, "stages": [
            {"label": l, "displayOrder": i, "metadata": {"probability": str(pr), **({"isClosed": "true"} if l in ("Won", "Lost") else {})}}
            for i, (l, pr) in enumerate(STAGES)]}
        code, r = call("POST", "/crm/v3/pipelines/deals", body)
        print("pipeline Social selling:", f"created {r.get('id')}" if code == 201 else f"HTTP {code} {r.get('message', '')[:300]}")
        pipe = r if code == 201 else None
    if pipe:
        print("stages:", {s["label"]: s["id"] for s in pipe["stages"]})


if __name__ == "__main__":
    main()
