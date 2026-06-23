package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.util.Try

/**
 * Crypto Fear & Greed Index enrichment — FREE, no API key (alternative.me). A KEYLESS as-of join: the right
 * side is one global daily series (no entity key), joined to each row by date. With an `asof` column it
 * returns the index AS OF that day (backward, no look-ahead via [[AsOf.pick]]); otherwise the latest value.
 *
 * Pipeline YAML:
 * {{{
 * - stage: fearGreed
 *   args: [{ asof: date }]   # or positional ["date"]
 * }}}
 * Adds fng_value, fng_class.
 */
class FearGreedStage extends WPartitionStage {

  override def name: String = "fearGreed"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val spec = JoinSpec.keyless(args, "date")
    lazy val series: Vector[(Long, (String, String))] = Try(FearGreedStage.fetchSeries(ctx)).getOrElse(Vector.empty)
    lazy val latest: Option[(String, String)] = series.sortBy(_._1).lastOption.map(_._2)
    rows.map { row =>
      val date = spec.asof.flatMap(c => row.str(c)).map(_.trim).getOrElse("")
      val hit = if (date.nonEmpty) AsOf.pick(series, date).map(_._2) else latest
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

  /** the whole global history as (epochSeconds, (value, classification)). */
  def fetchSeries(ctx: WebroStageContext): Vector[(Long, (String, String))] = {
    val body = Try(ctx.httpGet("https://api.alternative.me/fng/?limit=0", Map("Accept" -> "application/json"), 45000)).getOrElse("")
    ENTRY.findAllMatchIn(body).map(m => (m.group(3).toLong, (m.group(1), m.group(2)))).toVector
  }
}
