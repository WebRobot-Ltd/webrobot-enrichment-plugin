package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * World Bank Open Data enrichment — FREE, no API key. Adds country-level macro indicators to each row,
 * keyed by an ISO country code (ISO2 or ISO3). Uses `mrnev=1` (most-recent non-empty value) so each row
 * gets the latest available figure per indicator. Country-level macro context for global intelligence /
 * cross-market normalization.
 *
 * Context-aware PARTITION stage: each executor fetches+caches every country code it sees ONCE per
 * partition (no duplicate API calls). For each indicator adds `wb_<indicator>` + `wb_<indicator>_year`.
 *
 * Pipeline YAML:
 * {{{
 * - stage: worldBank
 *   args:
 *     - "country"                                    # ISO2/ISO3 country-code column (default "country")
 *     - "SP.POP.TOTL,NY.GDP.PCAP.CD,FP.CPI.TOTL.ZG"  # indicator codes (default: pop, GDP/capita, inflation)
 * }}}
 *
 * Parses the well-formed World Bank JSON with regex to stay dependency-free (SDK-only jar).
 */
class WorldBankStage extends WPartitionStage {

  override def name: String = "worldBank"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field = args.string(0, "country")
    val inds = args.string(1, "SP.POP.TOTL,NY.GDP.PCAP.CD,FP.CPI.TOTL.ZG")
      .split(",").map(_.trim).filter(_.nonEmpty).toVector
    val cache = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val code = row.str(field).getOrElse("").trim
      if (code.isEmpty) row
      else {
        val data = cache.getOrElseUpdate(code, Try(WorldBankStage.fetch(code, inds, ctx)).getOrElse(Map.empty))
        data.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
      }
    }
  }
}

object WorldBankStage {
  // World Bank data array element: {... "value":17500000, "date":"2022", ...}  (value may be null)
  private val VALUE = """"value"\s*:\s*(-?[\d.eE+]+)""".r
  private val DATE  = """"date"\s*:\s*"([^"]+)"""".r

  def fetch(code: String, inds: Vector[String], ctx: WebroStageContext): Map[String, Any] = {
    val c = java.net.URLEncoder.encode(code, "UTF-8")
    inds.flatMap { ind =>
      val url = s"https://api.worldbank.org/v2/country/$c/indicator/$ind?format=json&mrnev=1"
      val body = Try(ctx.httpGet(url, Map("Accept" -> "application/json"), 30000)).getOrElse("")
      // page metadata is the first JSON object; the observation (with "value") is in the second array
      if (body.isEmpty || body.contains("\"message\"")) Seq.empty
      else {
        val value = VALUE.findFirstMatchIn(body).map(_.group(1))
        val year  = DATE.findFirstMatchIn(body).map(_.group(1))
        // dots/specials are illegal in Spark/parquet column names → sanitize SP.POP.TOTL -> sp_pop_totl
        val col = "wb_" + ind.toLowerCase.replaceAll("[^a-z0-9]+", "_")
        value.toSeq.flatMap(v => Seq(col -> v) ++ year.map(y => s"${col}_year" -> y))
      }
    }.toMap
  }
}
