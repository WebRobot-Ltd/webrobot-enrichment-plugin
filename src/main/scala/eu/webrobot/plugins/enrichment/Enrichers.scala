package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * Enrichment helpers — the shared "mark the data to augment, then augment it" machinery that every
 * enrichment stage repeats by hand. A concrete enricher only declares WHICH field marks the lookup key and
 * a function that returns the augmentation columns; these traits handle the per-partition de-dup cache, the
 * empty/None-key skip, the failure isolation (a failed lookup leaves the row untouched, never breaks the
 * partition), and the `row.set` of every returned column.
 *
 * Backends are pluggable: the `enrich` function can hit an HTTP API (ctx.httpGet), an in-process model
 * (e.g. ONNX sentiment via the runtime-provided onnxruntime), ctx.llm, or pure local logic (lexicon).
 */

/**
 * Equi-join enricher: augment each row by a SINGLE key column (country, coin, protocol, address, …).
 * The key column is the join `on` of the YAML formalism — `args: [{ on: <col>, …extras }]` or positional
 * `["<col>"]`. A concrete stage declares the default key column and the per-key `enrich`; the trait handles
 * the per-partition de-dup cache, the empty-key skip, failure isolation, and the row.set.
 */
trait KeyedEnricher extends WPartitionStage {

  /** default key column when the YAML omits `on` (also the positional arg(0) name). */
  protected def defaultKey: String

  /** columns to add for one key value (cached once per distinct key). Empty map ⇒ leave the row as-is.
   * Read any stage-specific extras from `args` (map form `JoinSpec.from(args,…).extra(name)` or positional). */
  protected def enrich(key: String, args: WArgs, ctx: WebroStageContext): Map[String, Any]

  final override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val on    = JoinSpec.from(args, defaultKey).on
    val cache = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val key = row.str(on).map(_.trim).getOrElse("")
      if (key.isEmpty) row
      else Enrichers.augment(row, cache.getOrElseUpdate(key, Enrichers.safe(enrich(key, args, ctx))))
    }
  }
}

/** Augment each row from MULTIPLE columns (composite key: lat+lon, token+date, symbol+date, …). */
trait RowEnricher extends WPartitionStage {

  /** a stable de-dup key for the row's inputs (e.g. s"$lat,$lon"); "" ⇒ skip the row. */
  protected def cacheKey(row: WRow, args: WArgs): String

  /** columns to add for this row (cached by cacheKey). Empty map ⇒ leave the row as-is. */
  protected def enrich(row: WRow, args: WArgs, ctx: WebroStageContext): Map[String, Any]

  final override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val cache = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val ck = Try(cacheKey(row, args)).getOrElse("")
      if (ck.isEmpty) row
      else Enrichers.augment(row, cache.getOrElseUpdate(ck, Enrichers.safe(enrich(row, args, ctx))))
    }
  }
}

object Enrichers {
  /** run a lookup, swallowing failures into "no augmentation" (an enricher must never break a partition). */
  def safe(f: => Map[String, Any]): Map[String, Any] = Try(f).getOrElse(Map.empty)

  /** set every (column, value) of the augmentation onto the row. */
  def augment(row: WRow, cols: Map[String, Any]): WRow =
    cols.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
}
