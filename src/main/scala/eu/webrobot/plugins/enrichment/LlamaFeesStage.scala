package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * DefiLlama protocol fees & revenue enrichment — FREE, no API key. Fees/revenue are a protocol's on-chain
 * "earnings" — the basis for fundamental token valuation (price-to-fees / price-to-revenue).
 *
 * POINT-IN-TIME (no look-ahead): with a date column the stage reads the protocol's historical
 * `totalDataChart` and returns the daily fees AS OF that date plus the trailing-30d sum — so a backtest
 * row dated 2024-01-01 sees only fees up to 2024-01-01. Without a date column it returns the current
 * total24h / total7d / total30d aggregates (live use).
 *
 * Pipeline YAML:
 * {{{
 * - stage: llamaFees
 *   args:
 *     - "protocol"    # protocol-slug column (default "protocol")
 *     - "date"        # date column YYYY-MM-DD for point-in-time (default "date"; missing → current)
 *     - "dailyFees"   # dataType: dailyFees | dailyRevenue (default "dailyFees")
 * }}}
 * Adds (point-in-time) fees_daily + fees_30d + fees_date, or (current) fees_24h + fees_7d + fees_30d.
 * Prefix is fees_ for dailyFees and rev_ for dailyRevenue.
 */
class LlamaFeesStage extends WPartitionStage {

  override def name: String = "llamaFees"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field = args.string(0, "protocol")
    val dateF = args.string(1, "date")
    val dtype = args.string(2, "dailyFees")
    val px    = if (dtype.toLowerCase.contains("revenue")) "rev_" else "fees_"
    val chart = mutable.Map.empty[String, Vector[(Long, Double)]]
    val cur   = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val slug = row.str(field).getOrElse("").trim.toLowerCase
      val date = row.str(dateF).getOrElse("").trim
      if (slug.isEmpty) row
      else if (date.nonEmpty) {
        val series = chart.getOrElseUpdate(slug, Try(LlamaFeesStage.history(slug, dtype, ctx)).getOrElse(Vector.empty))
        LlamaFeesStage.asOf(series, date) match {
          case Some((ts, daily, sum30)) =>
            row.set(s"${px}daily", f"$daily%.2f").set(s"${px}30d", f"$sum30%.2f")
               .set(s"${px}date", LlamaFeesStage.toDate(ts))
          case None => row
        }
      } else {
        val d = cur.getOrElseUpdate(slug, Try(LlamaFeesStage.current(slug, dtype, px, ctx)).getOrElse(Map.empty))
        d.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
      }
    }
  }
}

object LlamaFeesStage {
  // historical daily series in totalDataChart: [[<unixSeconds>, <value>], ...]
  private val PT = """\[\s*(\d{9,})\s*,\s*([\d.eE+]+)\s*\]""".r
  private def num(body: String, key: String) =
    new scala.util.matching.Regex("\"" + key + "\"\\s*:\\s*([\\d.eE+]+)").findFirstMatchIn(body).map(_.group(1))

  def current(slug: String, dtype: String, px: String, ctx: WebroStageContext): Map[String, Any] = {
    val body = fetch(slug, dtype, ctx)
    if (!body.contains("total24h")) Map.empty
    else Seq(num(body, "total24h").map(s"${px}24h" -> _),
             num(body, "total7d").map(s"${px}7d" -> _),
             num(body, "total30d").map(s"${px}30d" -> _)).flatten.toMap
  }

  def history(slug: String, dtype: String, ctx: WebroStageContext): Vector[(Long, Double)] =
    PT.findAllMatchIn(fetch(slug, dtype, ctx)).map(m => (m.group(1).toLong, m.group(2).toDouble)).toVector

  /** (ts, daily-fees-as-of, trailing-30d-sum) at the cutoff day, no look-ahead. */
  def asOf(series: Vector[(Long, Double)], date: String): Option[(Long, Double, Double)] = {
    val cutoff = Try(java.time.LocalDate.parse(date).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond).getOrElse(Long.MaxValue)
    val past = series.filter(_._1 <= cutoff).sortBy(_._1)
    past.lastOption.map { case (ts, daily) =>
      val from = ts - 30L * 86400
      (ts, daily, past.filter(_._1 > from).map(_._2).sum)
    }
  }
  private def fetch(slug: String, dtype: String, ctx: WebroStageContext): String =
    Try(ctx.httpGet(s"https://api.llama.fi/summary/fees/$slug?dataType=$dtype",
      Map("Accept" -> "application/json"), 45000)).getOrElse("")
  def toDate(ts: Long): String =
    java.time.Instant.ofEpochSecond(ts).atZone(java.time.ZoneOffset.UTC).toLocalDate.toString
}
