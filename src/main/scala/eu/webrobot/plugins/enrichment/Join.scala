package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * Enrichment-as-JOIN model. An enrichment is a join between the input rows (left) and a reference source
 * (right) under an explicit CRITERION. Three criteria occur in practice:
 *   - equi-join     : right where right.key == left.on                     (worldBank, gleif, countryInfo…)
 *   - as-of join    : right = argmax{ t ≤ left.asof } per key  (BACKWARD)  (llamaPrice, chainTvl, fees, F&G…)
 *   - spatial/agg   : nearest / contains / windowed aggregate              (openMeteo, overpass, landRegistry)
 *
 * The criterion is declared once in the pipeline YAML and interpreted once here — so point-in-time
 * (no-look-ahead) correctness lives in a single place instead of being re-derived (and mis-derived) per stage.
 *
 * YAML formalism (a map element in `args`, with positional fallback):
 * {{{
 *   args: [{ on: <keyCol>, asof: <dateCol>, direction: backward }]   # asof omitted ⇒ current/equi
 *   args: ["<keyCol>", "<dateCol>"]                                   # positional equivalent
 * }}}
 */
final case class JoinSpec(on: String, asof: Option[String], direction: String, extras: Map[String, String]) {
  def extra(name: String, default: String = ""): String = extras.getOrElse(name, default)
  def isAsOf: Boolean = asof.isDefined
}

object JoinSpec {
  /** parse the join formalism from args[0] (map form) or positional args[0]=on, args[1]=asof. */
  def from(args: WArgs, defaultOn: String, defaultAsof: String = "date"): JoinSpec =
    asMap(args.opt(0)) match {
      case Some(m) =>
        val on   = m.getOrElse("on", defaultOn)
        val asof = m.get("asof").map(_.trim).filter(_.nonEmpty)
        JoinSpec(on, asof, m.getOrElse("direction", "backward"), m - "on" - "asof" - "direction")
      case None =>
        val on   = args.string(0, defaultOn)
        val asof = Option(args.string(1, "")).map(_.trim).filter(_.nonEmpty)
        JoinSpec(on, asof, "backward", Map.empty)
    }

  /** keyless variant: the right side has no entity key (global series, e.g. Fear&Greed) — only a temporal
   * as-of column. Positional arg(0) is the asof column (not a key). */
  def keyless(args: WArgs, defaultAsof: String = "date"): JoinSpec =
    asMap(args.opt(0)) match {
      case Some(m) =>
        JoinSpec("", m.get("asof").map(_.trim).filter(_.nonEmpty), m.getOrElse("direction", "backward"),
          m - "on" - "asof" - "direction")
      case None =>
        JoinSpec("", Option(args.string(0, defaultAsof)).map(_.trim).filter(_.nonEmpty), "backward", Map.empty)
    }

  /** coerce a YAML map element (scala Map or java.util.Map) to Map[String,String]; non-maps ⇒ None. */
  private def asMap(v: Option[Any]): Option[Map[String, String]] = v.collect {
    case m: scala.collection.Map[_, _] => m.map { case (k, vv) => k.toString -> str(vv) }.toMap
    case m: java.util.Map[_, _] =>
      import scala.jdk.CollectionConverters._
      m.asScala.map { case (k, vv) => k.toString -> str(vv) }.toMap
  }
  private def str(v: Any): String = v match { case null => ""; case x => x.toString }
}

/** The one correct point-in-time pick: latest reading at-or-before the cutoff day. Never returns a future
 * reading (that would be look-ahead) and never a fabricated fallback — `None` means "no data yet at that date". */
object AsOf {
  def parseDay(date: String): Option[Long] =
    Try(java.time.LocalDate.parse(date.trim).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond).toOption

  /** backward as-of over a (epochSeconds, payload) series. */
  def pick[A](series: Vector[(Long, A)], date: String): Option[(Long, A)] =
    parseDay(date).flatMap(cutoff => series.filter(_._1 <= cutoff).sortBy(_._1).lastOption)

  def toDate(ts: Long): String =
    java.time.Instant.ofEpochSecond(ts).atZone(java.time.ZoneOffset.UTC).toLocalDate.toString
}

/**
 * Base for the common case: the right side is a per-key time series; each row joins to its as-of point
 * (or, with no `asof` column, the latest point = current). De-dup cache per key, failure-isolated,
 * no-look-ahead — all centralized. A concrete stage only fetches the series and names its columns.
 */
trait AsOfEnricher extends WPartitionStage {
  protected def defaultOn: String
  protected def defaultAsof: String = "date"

  /** right side: (epochSeconds, columns-to-add) for one key. Sorting/age handled by AsOf. */
  protected def series(key: String, spec: JoinSpec, ctx: WebroStageContext): Vector[(Long, Map[String, Any])]

  final override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val spec  = JoinSpec.from(args, defaultOn, defaultAsof)
    val cache = mutable.Map.empty[String, Vector[(Long, Map[String, Any])]]
    rows.map { row =>
      val key = row.str(spec.on).map(_.trim).getOrElse("")
      if (key.isEmpty) row
      else {
        val s = cache.getOrElseUpdate(key, Try(series(key, spec, ctx)).getOrElse(Vector.empty))
        val cols = spec.asof match {
          case Some(col) => AsOf.pick(s, row.str(col).getOrElse("")).map(_._2)
          case None      => s.sortBy(_._1).lastOption.map(_._2) // current = latest reading
        }
        cols.map(Enrichers.augment(row, _)).getOrElse(row)
      }
    }
  }
}
