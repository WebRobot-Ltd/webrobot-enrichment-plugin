package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WebroStageContext}

import scala.util.Try

/**
 * CoinGecko market-data enrichment — FREE, no API key. Adds current price, market cap, 24h volume and
 * 24h change to each row keyed by a CoinGecko coin id (e.g. "bitcoin", "ethereum"). LIVE data — for
 * point-in-time historical price in a backtest use the `binancePrice` stage (CoinGecko's history endpoint
 * is key-gated).
 *
 * Pipeline YAML:
 * {{{
 * - stage: coinGecko
 *   args: [{ on: coin }]   # coin-id column (default "coin")
 * }}}
 * Adds coin_price_usd, coin_mcap, coin_volume, coin_change24h.
 */
class CoinGeckoStage extends KeyedEnricher {

  override def name: String = "coinGecko"

  override protected def defaultKey: String = "coin"

  override protected def enrich(key: String, args: WArgs, ctx: WebroStageContext): Map[String, Any] =
    CoinGeckoStage.fetch(key.toLowerCase, ctx)
}

object CoinGeckoStage {
  private val PRICE  = """"current_price"\s*:\s*([\d.]+)""".r
  private val MCAP   = """"market_cap"\s*:\s*(\d+)""".r
  private val VOL    = """"total_volume"\s*:\s*(\d+)""".r
  private val CHG    = """"price_change_percentage_24h"\s*:\s*(-?[\d.]+)""".r

  def fetch(id: String, ctx: WebroStageContext): Map[String, Any] = {
    val c = java.net.URLEncoder.encode(id, "UTF-8")
    val body = Try(ctx.httpGet(s"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=$c",
      Map("Accept" -> "application/json"), 30000)).getOrElse("")
    if (body.isEmpty || body.length < 5 || body.startsWith("{\"error")) Map.empty
    else Seq(
      PRICE.findFirstMatchIn(body).map("coin_price_usd" -> _.group(1)),
      MCAP.findFirstMatchIn(body).map("coin_mcap" -> _.group(1)),
      VOL.findFirstMatchIn(body).map("coin_volume" -> _.group(1)),
      CHG.findFirstMatchIn(body).map("coin_change24h" -> _.group(1))
    ).flatten.toMap
  }
}
