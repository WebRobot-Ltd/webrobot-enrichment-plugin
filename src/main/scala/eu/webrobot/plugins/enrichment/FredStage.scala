package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * FRED (St. Louis Fed) enrichment — US macro time series. Requires a FREE API key (env `FRED_API_KEY`,
 * or passed as the last arg). Adds the value of a FRED series to each row, observed at/just-before the
 * row's date (point-in-time, no look-ahead). With no date column, adds the latest available value to all
 * rows.
 *
 * Pipeline YAML:
 * {{{
 * - stage: fred
 *   args:
 *     - "GDPC1"          # FRED series id (e.g. Real GDP)
 *     - "date"           # date column (YYYY-MM-DD) for point-in-time, or "" for latest (default "date")
 *     - "fred_value"     # output column (default "fred_value")
 *     - ""               # api_key (optional; default reads env FRED_API_KEY)
 * }}}
 * Parses the FRED JSON with regex to stay dependency-free (SDK-only jar).
 */
class FredStage extends WPartitionStage {

  override def name: String = "fred"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val series   = args.string(0, "")
    val dateField = args.string(1, "date")
    val outCol   = args.string(2, "fred_value")
    val apiKey   = {
      val a = args.string(3, "")
      if (a.nonEmpty) a else Option(System.getenv("FRED_API_KEY")).getOrElse("")
    }
    if (series.isEmpty || apiKey.isEmpty) {
      ctx.warn(s"[$name] missing series or FRED_API_KEY — passthrough")
      return rows
    }
    val cache = mutable.Map.empty[String, Option[String]]
    rows.map { row =>
      val date = row.str(dateField).getOrElse("").trim // "" => latest
      val v = cache.getOrElseUpdate(date,
        Try(FredStage.fetch(series, apiKey, date, ctx)).toOption.flatten)
      v.map(value => row.set(outCol, value)).getOrElse(row)
    }
  }
}

object FredStage {
  private val VALUE = """"value"\s*:\s*"([^"]+)"""".r
  def fetch(series: String, apiKey: String, date: String, ctx: WebroStageContext): Option[String] = {
    val base = s"https://api.stlouisfed.org/fred/series/observations?series_id=$series" +
      s"&api_key=$apiKey&file_type=json&sort_order=desc&limit=1"
    val url = if (date.nonEmpty) s"$base&observation_end=${java.net.URLEncoder.encode(date, "UTF-8")}" else base
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/json"), 30000)).getOrElse("")
    if (body.isEmpty) None
    else VALUE.findFirstMatchIn(body).map(_.group(1)).filter(v => v != "." && v.nonEmpty)
  }
}
