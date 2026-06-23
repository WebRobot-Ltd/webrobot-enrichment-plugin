package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * DefiLlama TVL enrichment — FREE, no API key. Adds a protocol's Total Value Locked (USD) to each row,
 * keyed by the DefiLlama protocol slug (e.g. "aave", "uniswap"). Point-in-time aware: with a date column
 * it returns the TVL as of that date (no look-ahead) from the protocol's historical series; otherwise the
 * current TVL.
 *
 * Pipeline YAML:
 * {{{
 * - stage: defiLlama
 *   args:
 *     - "protocol"    # protocol-slug column (default "protocol")
 *     - "date"        # date column YYYY-MM-DD for point-in-time (default "date"; missing → current TVL)
 * }}}
 * Adds tvl_usd (and tvl_date when point-in-time).
 */
class DefiLlamaStage extends WPartitionStage {

  override def name: String = "defiLlama"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field = args.string(0, "protocol")
    val dateF = args.string(1, "date")
    // cache the full historical series per protocol (one fetch) for point-in-time lookups
    val hist = mutable.Map.empty[String, Vector[(Long, Double)]]
    val cur  = mutable.Map.empty[String, Option[String]]
    rows.map { row =>
      val slug = row.str(field).getOrElse("").trim.toLowerCase
      val date = row.str(dateF).getOrElse("").trim
      if (slug.isEmpty) row
      else if (date.nonEmpty) {
        val series = hist.getOrElseUpdate(slug, Try(DefiLlamaStage.history(slug, ctx)).getOrElse(Vector.empty))
        DefiLlamaStage.asOf(series, date) match {
          case Some((ts, tvl)) => row.set("tvl_usd", f"$tvl%.2f").set("tvl_date", DefiLlamaStage.toDate(ts))
          case None            => row
        }
      } else {
        val v = cur.getOrElseUpdate(slug, Try(DefiLlamaStage.current(slug, ctx)).toOption.flatten)
        v.map(t => row.set("tvl_usd", t)).getOrElse(row)
      }
    }
  }
}

object DefiLlamaStage {
  private val POINT = """"date"\s*:\s*(\d+)\s*,\s*"totalLiquidityUSD"\s*:\s*([\d.eE+]+)""".r

  def current(slug: String, ctx: WebroStageContext): Option[String] = {
    val body = Try(ctx.httpGet(s"https://api.llama.fi/tvl/$slug", Map("Accept" -> "application/json"), 30000)).getOrElse("")
    if (body.nonEmpty && body.matches("[\\d.eE+]+")) Some(body.trim) else None
  }

  def history(slug: String, ctx: WebroStageContext): Vector[(Long, Double)] = {
    val body = Try(ctx.httpGet(s"https://api.llama.fi/protocol/$slug", Map("Accept" -> "application/json"), 45000)).getOrElse("")
    POINT.findAllMatchIn(body).map(m => (m.group(1).toLong, m.group(2).toDouble)).toVector
  }

  /** latest point with date <= the requested day (point-in-time, no look-ahead). */
  def asOf(series: Vector[(Long, Double)], date: String): Option[(Long, Double)] = {
    val cutoff = Try(java.time.LocalDate.parse(date).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond).getOrElse(Long.MaxValue)
    series.filter(_._1 <= cutoff).sortBy(_._1).lastOption.orElse(series.sortBy(_._1).headOption)
  }
  def toDate(ts: Long): String =
    java.time.Instant.ofEpochSecond(ts).atZone(java.time.ZoneOffset.UTC).toLocalDate.toString
}
