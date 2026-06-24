package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WRow, WSourceStage, WebroStageContext}

import scala.util.Try
import scala.util.matching.Regex

/**
 * httpSource — a GENERIC, declarative HTTP/JSON source the AGENT programs from the pipeline YAML, with NO
 * Scala build. The upstream ("a monte") counterpart of python_row_transform: instead of hard-coding a Scala
 * stage per REST API, the agent supplies a spec (URL + per-field extraction) and this producer fetches and
 * emits rows. Zero-dependency — extraction is regex (capture group 1), the same proven approach as the
 * curated enrichers; a JSONPath variant belongs in a core stage where a JSON lib is available.
 *
 * Pipeline YAML:
 * {{{
 * - stage: httpSource
 *   args:
 *     - url: "https://api.the-odds-api.com/v4/sports/soccer_epl/odds?regions=eu&markets=h2h&apiKey=${THEODDS_API_KEY}"
 *       headers: { Accept: application/json }          # optional; ${ENV} expanded in url + header values
 *       record: '\{"id":"[^"]+".*?"bookmakers":\[.*?\]\}'   # optional regex splitting body into N records
 *       fields:                                        # name -> regex (capture group 1) applied per record
 *         event_id:  '"id":"([^"]+)"'
 *         home_team: '"home_team":"([^"]+)"'
 *         away_team: '"away_team":"([^"]+)"'
 *       timeout: 30000
 * }}}
 * `${VAR}` in the url/headers is substituted from the executor ENV (e.g. a key injected from a
 * cloud_credential). With no `record`, the whole body is one record (one row).
 */
class HttpSourceStage extends WSourceStage {

  override def name: String = "httpSource"

  override def produce(args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val spec = HttpSourceStage.asMap(args.opt(0).orNull)
    val url  = HttpSourceStage.expandEnv(spec.get("url").map(_.toString).getOrElse(""))
    if (url.isEmpty) Iterator.empty
    else {
      val headers = Map("Accept" -> "application/json") ++
        HttpSourceStage.asStrMap(spec.getOrElse("headers", null)).map { case (k, v) => k -> HttpSourceStage.expandEnv(v) }
      val timeout = spec.get("timeout").flatMap(v => Try(v.toString.toInt).toOption).getOrElse(30000)
      val body = Try(ctx.httpGet(url, headers, timeout)).getOrElse("")
      if (body.isEmpty) Iterator.empty
      else {
        val fields: Vector[(String, Regex)] =
          HttpSourceStage.asStrMap(spec.getOrElse("fields", null)).iterator
            .flatMap { case (k, rx) => Try(k -> rx.r).toOption }.toVector
        val blocks: Iterator[String] =
          spec.get("record").map(_.toString).filter(_.nonEmpty).flatMap(r => Try(r.r).toOption) match {
            case Some(rx) => rx.findAllMatchIn(body).map(_.matched)
            case None     => Iterator.single(body)
          }
        blocks.map { blk =>
          fields.foldLeft(WRow.empty) { case (row, (nm, rx)) =>
            rx.findFirstMatchIn(blk) match {
              case Some(m) => row.set(nm, if (m.groupCount >= 1) m.group(1) else m.matched)
              case None    => row
            }
          }
        }
      }
    }
  }
}

object HttpSourceStage {
  private val ENV = """\$\{([A-Za-z0-9_]+)\}""".r
  def expandEnv(s: String): String =
    ENV.replaceAllIn(s, m => java.util.regex.Matcher.quoteReplacement(sys.env.getOrElse(m.group(1), "")))

  def asMap(v: Any): Map[String, Any] = v match {
    case null => Map.empty
    case m: scala.collection.Map[_, _] => m.map { case (k, x) => k.toString -> x }.toMap
    case m: java.util.Map[_, _] =>
      import scala.jdk.CollectionConverters._
      m.asScala.map { case (k, x) => k.toString -> x }.toMap
    case _ => Map.empty
  }
  def asStrMap(v: Any): Map[String, String] = asMap(v).map { case (k, x) => k -> String.valueOf(x) }
}
