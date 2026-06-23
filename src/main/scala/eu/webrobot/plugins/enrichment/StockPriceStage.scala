package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.WebroStageContext

import scala.util.Try

/**
 * Equity historical price enrichment via the Alpaca Market Data API — the broker/market-data source for
 * stock OHLCV (free key-less web sources are unusable from a datacenter: Stooq is JS-challenge-gated and
 * Yahoo 429s the cluster IP). Joins each row by US ticker to that day's split-adjusted daily bar, as-of the
 * row's date (backward, no look-ahead via [[AsOfEnricher]]) — the equity analog of llamaPrice. The same
 * Alpaca account later drives live execution for the trading_player.
 *
 * Credentials are read from the executor ENVIRONMENT (never the repo): `ALPACA_API_KEY` + `ALPACA_SECRET`.
 * With no credentials the stage is inert (adds nothing), so it is safe to ship before the key is wired.
 *
 * Pipeline YAML:
 * {{{
 * - stage: stockPrice
 *   args: [{ on: ticker, asof: date, start: "2015-01-01", adjustment: split }]
 *   # positional ["ticker","date"] also works; asof omitted ⇒ latest bar
 * }}}
 * Adds px_open, px_high, px_low, px_close, px_volume, px_date.
 */
class StockPriceStage extends AsOfEnricher {
  override def name: String = "stockPrice"
  override protected def defaultOn: String = "ticker"

  override protected def series(ticker: String, spec: JoinSpec, ctx: WebroStageContext): Vector[(Long, Map[String, Any])] =
    StockPriceStage.creds match {
      case None => Vector.empty // no key wired → inert
      case Some((key, secret)) =>
        val sym   = java.net.URLEncoder.encode(ticker.toUpperCase, "UTF-8")
        val start = spec.extra("start", "2015-01-01")
        val adj   = spec.extra("adjustment", "split")
        val url   = s"https://data.alpaca.markets/v2/stocks/$sym/bars?timeframe=1Day&start=$start&limit=10000&adjustment=$adj"
        val body = Try(ctx.httpGet(url,
          Map("APCA-API-KEY-ID" -> key, "APCA-API-SECRET-KEY" -> secret, "Accept" -> "application/json"), 45000)).getOrElse("")
        StockPriceStage.BAR.findAllMatchIn(body).flatMap { m =>
          val (c, h, l, o, date, v) = (m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6))
          AsOf.parseDay(date).map { ts =>
            ts -> Map[String, Any]("px_open" -> o, "px_high" -> h, "px_low" -> l, "px_close" -> c,
                                   "px_volume" -> v, "px_date" -> date)
          }
        }.toVector
    }
}

object StockPriceStage {
  /** (keyId, secret) from the executor env, or None when unset → stage stays inert. */
  def creds: Option[(String, String)] =
    for {
      k <- sys.env.get("ALPACA_API_KEY").map(_.trim).filter(_.nonEmpty)
      s <- sys.env.get("ALPACA_SECRET").map(_.trim).filter(_.nonEmpty)
    } yield (k, s)

  // Alpaca bar (fields alphabetical): {"c":185.64,"h":188.44,"l":183.89,"n":1,"o":187.15,"t":"2024-01-02T05:00:00Z","v":82488200,"vw":185.9}
  private val BAR =
    """"c"\s*:\s*([\d.]+)\s*,\s*"h"\s*:\s*([\d.]+)\s*,\s*"l"\s*:\s*([\d.]+)\s*,\s*"n"\s*:\s*\d+\s*,\s*"o"\s*:\s*([\d.]+)\s*,\s*"t"\s*:\s*"([\d-]+)T[^"]*"\s*,\s*"v"\s*:\s*(\d+)""".r
}
