# WebRobot Enrichment Plugin

Free, open-source **ETL enrichment stages** for the [WebRobot](https://webrobot.eu) platform that
augment your scraped data with **public, no-API-key data sources**. Built on the
[`webrobot-plugin-sdk`](https://maven.pkg.github.com/WebRobot-Ltd/webrobot-etl) only — no platform
internals — so it is fully decoupled, auditable, and runs the same on the lightweight (in-process) and
Spark (distributed) engines.

## Stages

### `landRegistry` — UK sold-price comparables (real-estate arbitrage)

Enriches each property row with **HM Land Registry** SOLD prices for its postcode — the arbitrage
ground-truth (a listing's *asking* price vs the recent *sold* prices in the **same postcode**: asking ≪
`sold_median` = real underpricing, not just cheaper than other asking prices). Free, no key
([Open Government Licence](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)).

```yaml
- stage: landRegistry
  args:
    - "postcode"     # the postcode column (default "postcode")
    - 100            # max sold records per postcode (default 100)
```

Adds: `sold_count`, `sold_median_price`, `sold_avg_price`, `sold_min_price`, `sold_max_price`,
`sold_latest_date`. Context-aware **partition stage** → each Spark executor fetches+caches every postcode
it sees once (distributed load, no duplicate calls).

### `satelliteIndices` — Sentinel-2 indices around an asset (climate & land risk)

For each row with latitude/longitude, searches the **Copernicus Sentinel-2 L2A** archive (Cloud-Optimized
GeoTIFFs on the AWS Registry of Open Data, catalogue [Earth Search](https://earth-search.aws.element84.com/v1))
and reads **only the 20 m tiles around the point** via HTTP range requests — no image download, no GDAL.
Clouds, shadows and snow are masked with the scene classification layer. One output row per usable pass.

```yaml
- stage: satelliteIndices
  args: ["lat", "lon", 250, "365d", "today", 40, 30]   # cols, half-side m, from, to, max cloud %, max passes
```

Adds: `sat_date`, `sat_scene`, `sat_cloud_pct`, `sat_valid_pct`, `sat_pixels`, `ndvi_mean`, `ndvi_p10`,
`ndvi_p90` (vegetation), `ndwi_mean` (open water), `ndmi_mean` (moisture / drought stress), `nbr_mean`
(burn), `water_pct`, `sat_status` (`ok` or why there is no pass), `sat_source`. Licence: free and open —
credit *"Contains modified Copernicus Sentinel data &lt;year&gt;"*.

### `gdelt` — daily world-news tone (sentiment signal)

Produces one row per day of **GDELT** news tone (or article volume) for a query — a point-in-time news
sentiment series to join/aggregate (e.g. feed a trading or market-intel pipeline). Free, no key.

```yaml
- stage: gdelt
  args:
    - "Bitcoin OR BTC"   # GDELT query
    - 180                # days back from today (default 180)
    - "timelinetone"     # timelinetone (tone) | timelinevol (volume)
    - "tone"             # output column (default: tone / volume)
```

Produces: `date`, `query`, `tone` (or `volume`).

## Why these are free

Both stages hit **public, key-less** government / open-data APIs (HM Land Registry Linked Data; the GDELT
Project DOC 2.0). No credentials, no per-call cost, and the data never leaves your cluster beyond the
outbound API call. JSON is parsed with regex so the jar carries **zero dependencies** beyond the SDK.

## Build

```bash
GITHUB_TOKEN=<token-with-read:packages> ./gradlew jar
# → build/libs/webrobot-enrichment-plugin-0.1.0.jar  (thin jar: your classes + META-INF/services)
```

## Use

Deploy the jar to a WebRobot ETL runtime (it is discovered via `ServiceLoader` by the plugin adapter),
then reference the stages by name in any pipeline YAML. See the WebRobot plugin docs for deployment.

## License

MIT — use freely. Contributions welcome.

## droneSurvey — simulated drone-fleet survey (ArduPilot SITL)

For each site (latitude/longitude) a small fleet of simulated copters takes off from a pad just south of the area,
surveys a disc of `radius_m` metres on a grid of photo points derived from altitude and camera field of view (split in
parallel, balanced strips), and lands back. One output row per acquisition: drone, point, position, altitude, heading,
ground speed, battery, simulated time, photo footprint (WKT) and ground sample distance.

The flight is real autopilot behaviour: ArduCopter Copter-4.7.3 in software-in-the-loop (statically linked, GPLv3,
built from the ArduPilot tag of the same name), spoken to over MAVLink 2 by a small hand-written codec in the stage —
no extra dependency. The camera is not simulated: an acquisition is where and what a photo would cover. The simulator
is downloaded once per JVM from the public tools bucket and verified against its sha256.

```yaml
- stage: droneSurvey
  args: ["lat", "lon", 300, 3, 80, 12, 70, 55, 10, "asset_id"]
```
