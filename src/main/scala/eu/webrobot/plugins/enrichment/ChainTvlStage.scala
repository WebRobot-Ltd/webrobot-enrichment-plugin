package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.WebroStageContext

import scala.util.Try

/**
 * DefiLlama chain-TVL enrichment — FREE, no API key. Total DeFi TVL (USD) of a blockchain, joined to each
 * row by chain name. Modeled as an as-of JOIN (see [[AsOfEnricher]]): with an `asof` column it returns the
 * chain TVL as of that day (backward, no look-ahead); otherwise the latest. Macro DeFi risk-regime signal.
 *
 * Pipeline YAML:
 * {{{
 * - stage: chainTvl
 *   args: [{ on: chain, asof: date }]   # or positional ["chain", "date"]
 * }}}
 * Adds chain_tvl_usd (and chain_tvl_date).
 */
class ChainTvlStage extends AsOfEnricher {
  override def name: String = "chainTvl"
  override protected def defaultOn: String = "chain"

  override protected def series(chain: String, spec: JoinSpec, ctx: WebroStageContext): Vector[(Long, Map[String, Any])] = {
    val c = java.net.URLEncoder.encode(chain, "UTF-8")
    val body = Try(ctx.httpGet(s"https://api.llama.fi/v2/historicalChainTvl/$c", Map("Accept" -> "application/json"), 45000)).getOrElse("")
    ChainTvlStage.POINT.findAllMatchIn(body).map { m =>
      val ts = m.group(1).toLong
      ts -> Map[String, Any]("chain_tvl_usd" -> f"${m.group(2).toDouble}%.2f", "chain_tvl_date" -> AsOf.toDate(ts))
    }.toVector
  }
}

object ChainTvlStage {
  private val POINT = """"date"\s*:\s*(\d+)\s*,\s*"tvl"\s*:\s*([\d.eE+]+)""".r
}
