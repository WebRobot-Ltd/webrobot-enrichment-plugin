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
