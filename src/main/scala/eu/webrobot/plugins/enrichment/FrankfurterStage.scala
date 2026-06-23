package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * Frankfurter (ECB reference rates) FX enrichment — FREE, no API key. Adds the exchange rate base→target
 * to each row, observed on the row's date (point-in-time, no look-ahead). With no date column, uses the
 * latest rate.
 *
 * Pipeline YAML:
 * {{{
 * - stage: frankfurter
 *   args:
 *     - "date"        # date column YYYY-MM-DD (default "date"; "" or missing → latest)
 *     - "EUR"         # base currency (default "EUR")
 *     - "USD"         # target currency (default "USD")
 * }}}
 * Adds fx_<base>_<target> (and ..._date).
 */
class FrankfurterStage extends WPartitionStage {

  override def name: String = "frankfurter"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val dateField = args.string(0, "date")
    val base      = args.string(1, "EUR").toUpperCase
    val target    = args.string(2, "USD").toUpperCase
    val col       = s"fx_${base.toLowerCase}_${target.toLowerCase}"
    val cache = mutable.Map.empty[String, Option[(String, String)]]
    rows.map { row =>
      val date = row.str(dateField).getOrElse("").trim
      val res = cache.getOrElseUpdate(date, Try(FrankfurterStage.fetch(date, base, target, ctx)).toOption.flatten)
      res match {
        case Some((rate, d)) => row.set(col, rate).set(s"${col}_date", d)
        case None            => row
      }
    }
  }
}

object FrankfurterStage {
  def fetch(date: String, base: String, target: String, ctx: WebroStageContext): Option[(String, String)] = {
    val day = if (date.nonEmpty) date else "latest"
    val url = s"https://api.frankfurter.dev/v1/$day?from=$base&to=$target"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/json"), 30000)).getOrElse("")
    if (body.isEmpty) None
    else {
      val rate = new scala.util.matching.Regex("\"" + java.util.regex.Pattern.quote(target) + "\"\\s*:\\s*([\\d.]+)")
        .findFirstMatchIn(body).map(_.group(1))
      val d = """"date"\s*:\s*"([\d-]+)"""".r.findFirstMatchIn(body).map(_.group(1)).getOrElse(day)
      rate.map(r => (r, d))
    }
  }
}
