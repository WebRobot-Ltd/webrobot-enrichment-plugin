package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk._

/**
 * Demonstrative RDD-manipulation / transformation stages — each one exercises a DIFFERENT Spark RDD
 * operation through an SDK stage type, so a single test pipeline can validate the whole distributed
 * surface end-to-end on a Spark job. The adapter bridges them:
 *   WTransformStage → map        (rowMultiply)
 *   WFilterStage    → filter      (filterGt)
 *   WExpandStage    → flatMap     (explodeCsv)
 *   WAggregateStage → reduceByKey (sumByKey)   — pairwise combine
 *   WGroupStage     → groupByKey  (countByKey) — full-group reduce
 *
 * NOTE: WAggregate/WGroup `groupBy(row)` takes NO args (SDK contract), so these two key on a fixed
 * column named "key" — name your grouping column "key" upstream (e.g. via a transform).
 */

/** map — multiply a numeric field by a factor into an output column. Usage: rowMultiply <field> <factor> [<out>] */
class RowMultiplyStage extends WTransformStage {
  override def name: String = "rowMultiply"
  override def transform(row: WRow, args: WArgs): WRow = {
    val field  = args.string(0, "value")
    val factor = args.double(1, 2.0)
    val out    = args.string(2, s"${field}_x")
    row.double(field).fold(row)(v => row.set(out, v * factor))
  }
}

/** filter — keep rows where a numeric field is > threshold. Usage: filterGt <field> <threshold> */
class FilterGtStage extends WFilterStage {
  override def name: String = "filterGt"
  override def include(row: WRow, args: WArgs): Boolean = {
    val field = args.string(0, "value")
    val thr   = args.double(1, 0.0)
    row.double(field).exists(_ > thr)
  }
}

/** flatMap — split a delimited field into one row per token (the token replaces the field).
  * Usage: explodeCsv <field> [<delimiter=,>] */
class ExplodeCsvStage extends WExpandStage {
  override def name: String = "explodeCsv"
  override def expand(row: WRow, args: WArgs): Iterator[WRow] = {
    val field = args.string(0, "value")
    val delim = args.string(1, ",")
    row.str(field) match {
      case Some(s) if s.nonEmpty =>
        s.split(java.util.regex.Pattern.quote(delim)).iterator.map(_.trim).filter(_.nonEmpty)
          .map(tok => row.set(field, tok))
      case _ => Iterator.single(row)
    }
  }
}

/** reduceByKey — sum a value field across rows sharing the same "key". Usage: sumByKey [<valueField=value>] */
class SumByKeyStage extends WAggregateStage {
  override def name: String = "sumByKey"
  override def groupBy(row: WRow): String = row.str("key").getOrElse("")
  override def combine(left: WRow, right: WRow, args: WArgs): WRow = {
    val vf = args.string(0, "value")
    left.set(vf, left.double(vf).getOrElse(0.0) + right.double(vf).getOrElse(0.0))
  }
}

/** groupByKey — count rows per "key" → {key, count}. Usage: countByKey */
class CountByKeyStage extends WGroupStage {
  override def name: String = "countByKey"
  override def groupBy(row: WRow): String = row.str("key").getOrElse("")
  override def aggregate(rows: Iterable[WRow], args: WArgs, ctx: WebroStageContext): WRow =
    WRow(Map(
      "key"   -> rows.headOption.flatMap(_.str("key")).getOrElse(""),
      "count" -> rows.size))
}
