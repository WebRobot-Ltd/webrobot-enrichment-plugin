package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.WebroStageContext

import scala.util.Try

/**
 * DefiLlama protocol-TVL enrichment — FREE, no API key. A protocol's Total Value Locked (USD), joined to
 * each row by the DefiLlama protocol slug. Modeled as an as-of JOIN (see [[AsOfEnricher]]): with an `asof`
 * column it returns the TVL as of that day (backward, no look-ahead — `None`/untouched when the date
 * precedes the protocol's history, never a fabricated future value); otherwise the latest TVL.
 *
 * Pipeline YAML:
 * {{{
 * - stage: defiLlama
 *   args: [{ on: protocol, asof: date }]   # or positional ["protocol", "date"]
 * }}}
 * Adds tvl_usd (and tvl_date).
 */
class DefiLlamaStage extends AsOfEnricher {
  override def name: String = "defiLlama"
  override protected def defaultOn: String = "protocol"

  override protected def series(slug: String, spec: JoinSpec, ctx: WebroStageContext): Vector[(Long, Map[String, Any])] = {
    val s = slug.toLowerCase
    val body = Try(ctx.httpGet(s"https://api.llama.fi/protocol/$s", Map("Accept" -> "application/json"), 45000)).getOrElse("")
    DefiLlamaStage.POINT.findAllMatchIn(body).map { m =>
      val ts = m.group(1).toLong
      ts -> Map[String, Any]("tvl_usd" -> f"${m.group(2).toDouble}%.2f", "tvl_date" -> AsOf.toDate(ts))
    }.toVector
  }
}

object DefiLlamaStage {
  private val POINT = """"date"\s*:\s*(\d+)\s*,\s*"totalLiquidityUSD"\s*:\s*([\d.eE+]+)""".r
}
