package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * HM Land Registry SOLD-price enrichment — FREE, no API key (Open Government Licence). Adds per-postcode
 * SOLD comparables to each property row: the real-estate arbitrage ground-truth — a listing's asking
 * price vs the recent SOLD prices in the SAME postcode (asking ≪ sold_median = real underpricing, not
 * just vs other asking prices).
 *
 * Context-aware PARTITION stage: Spark partitions the rows across executors and each executor
 * fetches+caches every postcode it sees ONCE per partition (distributed load, no duplicate API calls).
 * Adds: sold_count, sold_median_price, sold_avg_price, sold_min_price, sold_max_price, sold_latest_date.
 *
 * Pipeline YAML:
 * {{{
 * - stage: landRegistry
 *   args:
 *     - "postcode"           # the postcode column (default "postcode")
 *     - 100                  # max sold records per postcode (default 100)
 * }}}
 *
 * Parses the well-formed Land Registry JSON with regex to stay dependency-free (SDK-only jar).
 */
class LandRegistryStage extends WPartitionStage {

  override def name: String = "landRegistry"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val pcField = args.string(0, "postcode")
    val limit   = args.int(1, 100)
    val cache = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val pc = row.str(pcField).getOrElse("").toUpperCase.trim.replaceAll("\\s+", " ")
      if (pc.isEmpty) row
      else {
        val stats = cache.getOrElseUpdate(pc, Try(LandRegistryStage.fetchStats(pc, limit, ctx)).getOrElse(Map.empty))
        stats.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
      }
    }
  }
}

object LandRegistryStage {
  private val PPI   = "http://landregistry.data.gov.uk/data/ppi/transaction-record.json"
  private val PRICE = """"pricePaid"\s*:\s*(\d+)""".r
  private val DATE  = """"transactionDate"\s*:\s*"([^"]+)"""".r

  def fetchStats(pc: String, limit: Int, ctx: WebroStageContext): Map[String, Any] = {
    val q = java.net.URLEncoder.encode(pc, "UTF-8")
    val url = s"$PPI?propertyAddress.postcode=$q&_pageSize=$limit&_sort=-transactionDate"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/json"), 40000)).getOrElse("")
    if (body.isEmpty || !body.trim.startsWith("{")) return Map.empty
    val prices = PRICE.findAllMatchIn(body).map(_.group(1).toLong).toVector.sorted
    if (prices.isEmpty) return Map.empty
    val n = prices.length
    val median = if (n % 2 == 1) prices(n / 2).toDouble else (prices(n / 2 - 1) + prices(n / 2)) / 2.0
    val latest = DATE.findFirstMatchIn(body).map(_.group(1)).getOrElse("")  // sorted desc → first = latest
    Map(
      "sold_count"        -> n.toString,
      "sold_median_price" -> math.round(median).toString,
      "sold_avg_price"    -> (prices.sum / n).toString,
      "sold_min_price"    -> prices.head.toString,
      "sold_max_price"    -> prices.last.toString,
      "sold_latest_date"  -> latest)
  }
}
