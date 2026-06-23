package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * Crypto Fear & Greed Index enrichment — FREE, no API key (alternative.me). Adds the index value (0-100)
 * and its classification to each row. Point-in-time aware: with a date column it returns the index AS OF
 * that day (no look-ahead) from the full historical series; otherwise the latest value.
 *
 * Pipeline YAML:
 * {{{
 * - stage: fearGreed
 *   args:
 *     - "date"        # date column YYYY-MM-DD for point-in-time (default "date"; missing → latest)
 * }}}
 * Adds fng_value, fng_class.
 */
class FearGreedStage extends WPartitionStage {

  override def name: String = "fearGreed"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val dateF = args.string(0, "date")
    // one fetch of the whole series (cached), then per-row point-in-time lookup
    lazy val series: Map[String, (String, String)] = Try(FearGreedStage.fetchSeries(ctx)).getOrElse(Map.empty)
    lazy val latest: Option[(String, String)] = series.toVector.sortBy(_._1).lastOption.map(_._2)
    rows.map { row =>
      val date = row.str(dateF).getOrElse("").trim
      val hit = if (date.nonEmpty) FearGreedStage.asOf(series, date) else latest
      hit match {
        case Some((value, cls)) => row.set("fng_value", value).set("fng_class", cls)
        case None               => row
      }
    }
  }
}

object FearGreedStage {
  // {"value": "23","value_classification": "Extreme Fear","timestamp": "1782172800",...}
  private val ENTRY =
    """"value"\s*:\s*"(\d+)"\s*,\s*"value_classification"\s*:\s*"([^"]+)"\s*,\s*"timestamp"\s*:\s*"(\d+)"""".r

  /** date(yyyy-MM-dd) -> (value, classification) for the whole history. */
  def fetchSeries(ctx: WebroStageContext): Map[String, (String, String)] = {
    val body = Try(ctx.httpGet("https://api.alternative.me/fng/?limit=0", Map("Accept" -> "application/json"), 45000)).getOrElse("")
    ENTRY.findAllMatchIn(body).map { m =>
      val date = java.time.Instant.ofEpochSecond(m.group(3).toLong).atZone(java.time.ZoneOffset.UTC).toLocalDate.toString
      date -> (m.group(1), m.group(2))
    }.toMap
  }

  def asOf(series: Map[String, (String, String)], date: String): Option[(String, String)] =
    series.get(date).orElse(series.filterKeys(_ <= date).toVector.sortBy(_._1).lastOption.map(_._2))
}
