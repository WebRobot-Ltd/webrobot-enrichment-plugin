package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * Binance USDT-M futures positioning enrichment — FREE, no API key. Adds the funding rate and open interest
 * for a perpetual symbol to each row — derivatives-market positioning, a leading risk/timing signal for a
 * crypto portfolio (extreme funding / OI = crowded trade).
 *
 * POINT-IN-TIME (no look-ahead): with a date column it returns the funding rate at that day and the open
 * interest from the daily history as of that day; otherwise the latest values.
 *
 * Pipeline YAML:
 * {{{
 * - stage: binanceFunding
 *   args:
 *     - "symbol"      # perp symbol column, e.g. BTCUSDT (default "symbol")
 *     - "date"        # date column YYYY-MM-DD for point-in-time (default "date"; missing → latest)
 * }}}
 * Adds funding_rate, mark_price, open_interest, oi_value.
 */
class BinanceFundingStage extends WPartitionStage {

  override def name: String = "binanceFunding"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val symF  = args.string(0, "symbol")
    val dateF = args.string(1, "date")
    val cache = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val sym  = row.str(symF).getOrElse("").trim.toUpperCase
      val date = row.str(dateF).getOrElse("").trim
      if (sym.isEmpty) row
      else {
        val d = cache.getOrElseUpdate(s"$sym|$date", Try(BinanceFundingStage.fetch(sym, date, ctx)).getOrElse(Map.empty))
        d.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
      }
    }
  }
}

object BinanceFundingStage {
  private val FR  = """"fundingRate"\s*:\s*"([-\d.]+)"""".r
  private val MP  = """"markPrice"\s*:\s*"([\d.]+)"""".r
  private val OI  = """"openInterest"\s*:\s*"([\d.]+)"""".r
  private val SOI = """"sumOpenInterest"\s*:\s*"([\d.]+)"""".r
  private val SOV = """"sumOpenInterestValue"\s*:\s*"([\d.]+)"""".r
  private val FAPI = "https://fapi.binance.com"

  def fetch(sym: String, date: String, ctx: WebroStageContext): Map[String, Any] = {
    val ms = if (date.nonEmpty)
      Try(java.time.LocalDate.parse(date).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond * 1000).getOrElse(0L) else 0L
    val frUrl =
      if (ms > 0) s"$FAPI/fapi/v1/fundingRate?symbol=$sym&startTime=$ms&limit=1"
      else s"$FAPI/fapi/v1/fundingRate?symbol=$sym&limit=1"
    val frBody = Try(ctx.httpGet(frUrl, Map("Accept" -> "application/json"), 30000)).getOrElse("")
    val funding = Seq(
      FR.findFirstMatchIn(frBody).map("funding_rate" -> _.group(1)),
      MP.findFirstMatchIn(frBody).map("mark_price" -> _.group(1))
    ).flatten

    val oi =
      if (ms > 0) {
        val body = Try(ctx.httpGet(s"$FAPI/futures/data/openInterestHist?symbol=$sym&period=1d&startTime=$ms&limit=1",
          Map("Accept" -> "application/json"), 30000)).getOrElse("")
        Seq(SOI.findFirstMatchIn(body).map("open_interest" -> _.group(1)),
            SOV.findFirstMatchIn(body).map("oi_value" -> _.group(1))).flatten
      } else {
        val body = Try(ctx.httpGet(s"$FAPI/fapi/v1/openInterest?symbol=$sym", Map("Accept" -> "application/json"), 30000)).getOrElse("")
        OI.findFirstMatchIn(body).map("open_interest" -> _.group(1)).toSeq
      }
    (funding ++ oi).toMap
  }
}
