package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * Eurostat enrichment — FREE, no API key. Adds a Eurostat indicator value (latest time period) to each
 * row keyed by a geo code (country ISO2 or NUTS region). Complements ISTAT on an EU-wide scale (regional
 * GDP, unemployment, prices, ...). Generic: pass the Eurostat dataset code + optional extra filter query.
 *
 * Context-aware PARTITION stage: each executor fetches+caches every (dataset,geo) it sees ONCE per
 * partition. Adds `<out>` (the value) and `<out>_period` (the time period, when present).
 *
 * Pipeline YAML (regional GDP, dataset nama_10r_2gdp):
 * {{{
 * - stage: eurostat
 *   args:
 *     - "geo"                # geo/NUTS code column (default "geo")
 *     - "nama_10r_2gdp"      # Eurostat dataset code
 *     - "eurostat_value"     # output column (default "eurostat_value")
 *     - "&unit=MIO_EUR"      # extra filter query appended verbatim (optional; include leading '&')
 * }}}
 *
 * Parses the JSON-stat 2.0 payload with regex to stay dependency-free (SDK-only jar).
 */
class EurostatStage extends WPartitionStage {

  override def name: String = "eurostat"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field   = args.string(0, "geo")
    val dataset = args.string(1, "")
    val outCol  = args.string(2, "eurostat_value")
    val extra   = args.string(3, "")
    val cache   = mutable.Map.empty[String, Option[(String, String)]]
    rows.map { row =>
      val geo = row.str(field).getOrElse("").trim
      if (geo.isEmpty || dataset.isEmpty) row
      else {
        val res = cache.getOrElseUpdate(geo,
          Try(EurostatStage.fetch(dataset, geo, extra, ctx)).toOption.flatten)
        res match {
          case Some((value, period)) => row.set(outCol, value).set(s"${outCol}_period", period)
          case None                  => row
        }
      }
    }
  }
}

object EurostatStage {
  private val BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
  // JSON-stat 2.0: "value":{"0":12345.6}  (single observation when lastTimePeriod=1 + fully filtered)
  private val VALUE  = """"value"\s*:\s*\{\s*"[0-9]+"\s*:\s*(-?[\d.eE+]+)""".r
  // time dimension category labels: "label":{"2023":"2023"} → grab the YYYY key (best-effort)
  private val PERIOD = """"((?:19|20)\d{2}(?:-?[A-Za-z0-9]+)?)"\s*:\s*\d+\s*[},]""".r

  def fetch(dataset: String, geo: String, extra: String, ctx: WebroStageContext): Option[(String, String)] = {
    val g = java.net.URLEncoder.encode(geo, "UTF-8")
    val url = s"$BASE/$dataset?format=JSON&lang=EN&lastTimePeriod=1&geo=$g$extra"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/json"), 45000)).getOrElse("")
    if (body.isEmpty || !body.contains("\"value\"")) None
    else VALUE.findFirstMatchIn(body).map { m =>
      val period = PERIOD.findFirstMatchIn(body).map(_.group(1)).getOrElse("")
      (m.group(1), period)
    }
  }
}
