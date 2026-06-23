package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WRow, WSourceStage, WebroStageContext}

import scala.util.Try

/**
 * GDELT DOC 2.0 news-tone SOURCE stage — FREE, no API key, point-in-time (the tone of day D reflects
 * only news published up to D). Produces one row per day: [date, query, tone|volume]. The dogfood news
 * signal — a WebRobot pipeline turns daily world-news sentiment into rows it can join/aggregate.
 *
 * Pipeline YAML:
 * {{{
 * - stage: gdelt
 *   args:
 *     - "Bitcoin OR BTC"     # GDELT query
 *     - 180                  # days back from today (default 180)
 *     - "timelinetone"       # timelinetone (tone) | timelinevol (article volume)
 *     - "tone"               # output column name (default: tone / volume)
 * }}}
 *
 * Parses the well-formed GDELT JSON with regex to stay dependency-free (SDK-only jar).
 */
class GdeltSourceStage extends WSourceStage {

  override def name: String = "gdelt"

  override def produce(args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val query = args.string(0, "")
    val days  = args.int(1, 180)
    val mode  = args.string(2, "timelinetone")
    val asCol = args.string(3, if (mode == "timelinetone") "tone" else "volume")
    if (query.trim.isEmpty) {
      ctx.warn(s"[$name] empty query — no rows"); return Iterator.empty
    }
    val (start, end) = GdeltSourceStage.window(days)
    val points = GdeltSourceStage.fetch(query, start, end, mode, ctx)
    ctx.log(s"[$name] '$query': ${points.size} daily $mode points")
    points.iterator.map { case (date, value) =>
      WRow(Map("date" -> date, "query" -> query, asCol -> value))
    }
  }
}

object GdeltSourceStage {
  private val DOC = "https://api.gdeltproject.org/api/v2/doc/doc"
  // {"date":"2024-01-15T00:00:00Z","value":-1.23}  (order is stable in the GDELT timeline payload)
  private val POINT = """"date"\s*:\s*"([^"]+)"\s*,\s*"value"\s*:\s*(-?\d+(?:\.\d+)?)""".r

  def window(days: Int): (String, String) = {
    val fmt = new java.text.SimpleDateFormat("yyyyMMddHHmmss")
    fmt.setTimeZone(java.util.TimeZone.getTimeZone("UTC"))
    val now = System.currentTimeMillis()
    (fmt.format(new java.util.Date(now - days.toLong * 86400000L)), fmt.format(new java.util.Date(now)))
  }

  /** Fetch via ctx.httpGet with a small 429-aware backoff (the free API throttles). Empty on failure. */
  def fetch(query: String, start: String, end: String, mode: String, ctx: WebroStageContext): Seq[(String, Double)] = {
    val q = java.net.URLEncoder.encode(query, "UTF-8")
    val url = s"$DOC?query=$q&mode=$mode&startdatetime=$start&enddatetime=$end&format=json"
    var attempt = 0
    while (attempt < 5) {
      attempt += 1
      val body = Try(ctx.httpGet(url, Map("User-Agent" -> "WebRobot ETL"), 40000)).getOrElse("")
      if (body.nonEmpty && body.trim.startsWith("{")) {
        return POINT.findAllMatchIn(body).map(m => (iso(m.group(1)), m.group(2).toDouble)).toVector
      }
      if (attempt < 5) Thread.sleep(math.min(8000L * attempt, 45000L))   // throttle/empty → back off
    }
    Seq.empty
  }

  private def iso(raw: String): String = {
    val s = raw.replace("-", "").replace(":", "").take(8)
    if (s.length == 8) s"${s.substring(0, 4)}-${s.substring(4, 6)}-${s.substring(6, 8)}" else raw
  }
}
