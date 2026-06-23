package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * DefiLlama protocol fees & revenue enrichment — FREE, no API key. Adds a protocol's fees over 24h / 7d /
 * 30d (USD) to each row keyed by the DefiLlama protocol slug. Fees/revenue are the on-chain "earnings" of a
 * protocol — the basis for fundamental token valuation (price-to-fees / price-to-revenue).
 *
 * Pipeline YAML:
 * {{{
 * - stage: llamaFees
 *   args:
 *     - "protocol"    # protocol-slug column (default "protocol")
 *     - "dailyFees"   # dataType: dailyFees | dailyRevenue (default "dailyFees")
 * }}}
 * Adds fees_24h, fees_7d, fees_30d (prefix matches the dataType: fees_* or rev_*).
 */
class LlamaFeesStage extends WPartitionStage {

  override def name: String = "llamaFees"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field = args.string(0, "protocol")
    val dtype = args.string(1, "dailyFees")
    val prefix = if (dtype.toLowerCase.contains("revenue")) "rev_" else "fees_"
    val cache = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val slug = row.str(field).getOrElse("").trim.toLowerCase
      if (slug.isEmpty) row
      else {
        val d = cache.getOrElseUpdate(slug, Try(LlamaFeesStage.fetch(slug, dtype, prefix, ctx)).getOrElse(Map.empty))
        d.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
      }
    }
  }
}

object LlamaFeesStage {
  private def num(body: String, key: String) =
    new scala.util.matching.Regex("\"" + key + "\"\\s*:\\s*([\\d.eE+]+)").findFirstMatchIn(body).map(_.group(1))

  def fetch(slug: String, dtype: String, prefix: String, ctx: WebroStageContext): Map[String, Any] = {
    val body = Try(ctx.httpGet(s"https://api.llama.fi/summary/fees/$slug?dataType=$dtype",
      Map("Accept" -> "application/json"), 30000)).getOrElse("")
    if (body.isEmpty || !body.contains("total24h")) Map.empty
    else Seq(
      num(body, "total24h").map(s"${prefix}24h" -> _),
      num(body, "total7d").map(s"${prefix}7d" -> _),
      num(body, "total30d").map(s"${prefix}30d" -> _)
    ).flatten.toMap
  }
}
