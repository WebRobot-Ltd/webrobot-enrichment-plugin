package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * DefiLlama token-price enrichment — FREE, no API key, with KEY-LESS HISTORICAL support (the differentiator
 * vs CoinGecko, whose history endpoint is key-gated). Adds the USD price of a token to each row, observed
 * AS OF the row's date (point-in-time, no look-ahead) when a date column is present, else the current price.
 *
 * The token id follows DefiLlama's "{chain}:{address}" or "coingecko:{id}" form, e.g. "coingecko:bitcoin",
 * "ethereum:0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".
 *
 * Pipeline YAML:
 * {{{
 * - stage: llamaPrice
 *   args:
 *     - "token"       # token-id column (default "token"; e.g. coingecko:bitcoin)
 *     - "date"        # date column YYYY-MM-DD for point-in-time (default "date"; missing → current)
 * }}}
 * Adds price_usd (and price_date).
 */
class LlamaPriceStage extends WPartitionStage {

  override def name: String = "llamaPrice"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field = args.string(0, "token")
    val dateF = args.string(1, "date")
    val cache = mutable.Map.empty[String, Option[(String, String)]]
    rows.map { row =>
      val token = row.str(field).getOrElse("").trim
      val date  = row.str(dateF).getOrElse("").trim
      if (token.isEmpty) row
      else {
        val res = cache.getOrElseUpdate(s"$token|$date",
          Try(LlamaPriceStage.fetch(token, date, ctx)).toOption.flatten)
        res match {
          case Some((price, d)) => row.set("price_usd", price).set("price_date", d)
          case None             => row
        }
      }
    }
  }
}

object LlamaPriceStage {
  private val PRICE = """"price"\s*:\s*([\d.]+)""".r
  private val TS    = """"timestamp"\s*:\s*(\d+)""".r

  def fetch(token: String, date: String, ctx: WebroStageContext): Option[(String, String)] = {
    val t = java.net.URLEncoder.encode(token, "UTF-8")
    val url =
      if (date.nonEmpty) {
        val ts = Try(java.time.LocalDate.parse(date).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond).getOrElse(0L)
        s"https://coins.llama.fi/prices/historical/$ts/$t"
      } else s"https://coins.llama.fi/prices/current/$t"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/json"), 30000)).getOrElse("")
    if (body.isEmpty || !body.contains("\"price\"")) None
    else PRICE.findFirstMatchIn(body).map { m =>
      val d = TS.findFirstMatchIn(body).map(t =>
        java.time.Instant.ofEpochSecond(t.group(1).toLong).atZone(java.time.ZoneOffset.UTC).toLocalDate.toString
      ).getOrElse(date)
      (m.group(1), d)
    }
  }
}
