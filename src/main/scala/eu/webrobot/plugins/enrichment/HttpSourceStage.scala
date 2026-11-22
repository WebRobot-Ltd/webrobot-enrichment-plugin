package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WRow, WSourceStage, WebroStageContext}

import java.io.{BufferedReader, InputStream, InputStreamReader, Reader}
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.Charset
import java.time.Duration
import java.util.zip.{GZIPInputStream, ZipInputStream}
import scala.util.Try
import scala.util.matching.Regex

/**
 * httpSource — a GENERIC, declarative HTTP source the AGENT programs from the pipeline YAML, with NO Scala build: a JSON/REST API (GET or POST, paged) or an open-data bulk file (CSV, optionally zipped or gzipped, streamed). The upstream ("a monte") counterpart of python_row_transform: instead of hard-coding a Scala stage per source, the agent supplies a spec and this producer fetches and emits rows. Zero-dependency: JSON extraction is regex (capture group 1); CSV is parsed by a small streaming reader, so a file of hundreds of MB never sits in memory and `filter` drops the rows the job does not need before Spark sees them.
 *
 * Pipeline YAML:
 * {{{
 * # 1) JSON API, GET
 * - stage: httpSource
 *   args:
 *     - url: "https://api.the-odds-api.com/v4/sports/soccer_epl/odds?regions=eu&markets=h2h&apiKey=${THEODDS_API_KEY}"
 *       headers: { Accept: application/json }          # optional; ${ENV} expanded in url, body and header values
 *       record: '\{"id":"[^"]+".*?"bookmakers":\[.*?\]\}'   # optional regex splitting body into N records
 *       fields:                                        # name -> regex (capture group 1) applied per record
 *         event_id:  '"id":"([^"]+)"'
 *         home_team: '"home_team":"([^"]+)"'
 *       timeout: 30000
 * # 2) JSON API, POST with paging ({page} is replaced by 1..pages; stops at the first page with no records)
 * - stage: httpSource
 *   args:
 *     - url: "https://api.ted.europa.eu/v3/notices/search"
 *       method: POST
 *       body: '{"query":"buyer-country=ITA AND classification-cpv=72*","fields":["publication-number","notice-title"],"limit":250,"page":{page}}'
 *       pages: 20
 *       record: '\{"publication-number".*?\}\}'
 *       fields: { publication_number: '"publication-number":"([^"]+)"' }
 * # 3) bulk CSV file(s), zipped or not, streamed and filtered
 * - stage: httpSource
 *   args:
 *     - urls: ["https://dati.anticorruzione.it/opendata/download/dataset/cig/filesystem/20261101-cig_csv.zip"]
 *       format: csv                 # zip/gz detected from the URL (or unzip: true / gzip: true)
 *       delimiter: ";"
 *       columns: [cig, oggetto_gara, importo_lotto, cod_cpv, provincia, data_pubblicazione]   # optional subset
 *       filter: { cod_cpv: '^72', provincia: 'MILANO|ROMA' }    # optional: column -> regex, all must match
 *       limit: 0                    # optional max rows per file (0 = all)
 *       sourceColumn: source_url    # optional: add the file URL to each row
 * }}}
 * `${VAR}` in url/body/headers is substituted from the executor ENV (e.g. a key injected from a
 * cloud_credential). With no `record`, the whole JSON body is one record (one row).
 */
class HttpSourceStage extends WSourceStage {

  override def name: String = "httpSource"

  override def produce(args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val spec = HttpSourceStage.asMap(args.opt(0).orNull)
    val urls = HttpSourceStage.urlsOf(spec)
    if (urls.isEmpty) Iterator.empty
    else {
      val headers = Map("Accept" -> "application/json") ++
        HttpSourceStage.asStrMap(spec.getOrElse("headers", null)).map { case (k, v) => k -> HttpSourceStage.expandEnv(v) }
      val timeout = spec.get("timeout").flatMap(v => Try(v.toString.toInt).toOption).getOrElse(30000)
      val format  = spec.get("format").map(_.toString.toLowerCase).getOrElse("json")
      if (format == "csv") urls.iterator.flatMap(u => HttpSourceStage.csvRows(u, spec, headers - "Accept", timeout))
      else urls.iterator.flatMap(u => HttpSourceStage.jsonRows(u, spec, headers, timeout, ctx))
    }
  }
}

object HttpSourceStage {
  private val ENV = """\$\{([A-Za-z0-9_]+)\}""".r
  private val UA  = "WebRobot-ETL/1.0 (+https://www.webrobot.eu)"

  def expandEnv(s: String): String =
    ENV.replaceAllIn(s, m => java.util.regex.Matcher.quoteReplacement(sys.env.getOrElse(m.group(1), "")))

  def asMap(v: Any): Map[String, Any] = v match {
    case null => Map.empty
    case m: scala.collection.Map[_, _] => m.map { case (k, x) => k.toString -> x }.toMap
    case m: java.util.Map[_, _] =>
      import scala.collection.JavaConverters._
      m.asScala.map { case (k, x) => k.toString -> x }.toMap
    case _ => Map.empty
  }
  def asStrMap(v: Any): Map[String, String] = asMap(v).map { case (k, x) => k -> String.valueOf(x) }

  def asList(v: Any): Seq[String] = v match {
    case null => Nil
    case s: String => Seq(s)
    case l: scala.collection.Seq[_] => l.map(String.valueOf).toList
    case l: java.util.List[_] =>
      import scala.collection.JavaConverters._
      l.asScala.map(String.valueOf).toList
    case a: Array[_] => a.map(String.valueOf).toList
    case x => Seq(String.valueOf(x))
  }

  def urlsOf(spec: Map[String, Any]): Seq[String] = {
    val many = asList(spec.getOrElse("urls", null))
    val all  = if (many.nonEmpty) many else asList(spec.getOrElse("url", null))
    all.map(u => expandEnv(u.trim)).filter(_.nonEmpty)
  }

  private def intOf(spec: Map[String, Any], k: String, d: Int): Int =
    spec.get(k).flatMap(v => Try(v.toString.trim.toDouble.toInt).toOption).getOrElse(d)
  private def boolOf(spec: Map[String, Any], k: String): Option[Boolean] =
    spec.get(k).map(v => String.valueOf(v).trim.toLowerCase).map(v => v == "true" || v == "1" || v == "yes")

  // ── JSON ────────────────────────────────────────────────────────────────────

  def jsonRows(url0: String, spec: Map[String, Any], headers: Map[String, String], timeout: Int,
               ctx: WebroStageContext): Iterator[WRow] = {
    val method = spec.get("method").map(_.toString.trim.toUpperCase).getOrElse("GET")
    val body0  = spec.get("body").map(b => expandEnv(String.valueOf(b))).getOrElse("")
    val pages  = math.max(1, intOf(spec, "pages", 1))
    val paged  = pages > 1 || url0.contains("{page}") || body0.contains("{page}")
    val fields: Vector[(String, Regex)] =
      asStrMap(spec.getOrElse("fields", null)).iterator.flatMap { case (k, rx) => Try(k -> rx.r).toOption }.toVector
    val recordRx: Option[Regex] = spec.get("record").map(_.toString).filter(_.nonEmpty).flatMap(r => Try(r.r).toOption)
    val hdr = if (method == "POST" && !headers.keys.exists(_.equalsIgnoreCase("Content-Type")))
      headers + ("Content-Type" -> "application/json") else headers

    def page(n: Int): Vector[WRow] = {
      val url  = url0.replace("{page}", n.toString)
      val body = body0.replace("{page}", n.toString)
      val text = Try(if (method == "POST") ctx.httpPost(url, body, hdr, timeout) else ctx.httpGet(url, hdr, timeout)).getOrElse("")
      if (text.isEmpty) Vector.empty
      else {
        val blocks: Iterator[String] = recordRx match {
          case Some(rx) => rx.findAllMatchIn(text).map(_.matched)
          case None     => Iterator.single(text)
        }
        blocks.map { blk =>
          fields.foldLeft(WRow.empty) { case (row, (nm, rx)) =>
            // every declared field is always a column (empty when the record lacks it): otherwise a field no
            // record has — e.g. the winner of notices that are all still open — is no column at all, and the
            // next sql_query fails on a column the pipeline itself declared
            rx.findFirstMatchIn(blk) match {
              case Some(m) => row.set(nm, if (m.groupCount >= 1) m.group(1) else m.matched)
              case None    => row.set(nm, "")
            }
          }
        }.toVector
      }
    }

    if (!paged) page(1).iterator
    else Iterator.from(1).take(pages).map(page).takeWhile(_.nonEmpty).flatten
  }

  // ── CSV (bulk open-data files) ──────────────────────────────────────────────

  private lazy val client: HttpClient =
    HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(Duration.ofSeconds(30)).build()

  private def open(url: String, headers: Map[String, String], timeout: Int): InputStream = {
    val b = HttpRequest.newBuilder(URI.create(url)).GET().timeout(Duration.ofMillis(math.max(timeout, 600000).toLong))
      .header("User-Agent", UA)
    headers.foreach { case (k, v) => if (!k.equalsIgnoreCase("User-Agent")) b.header(k, v) else b.setHeader(k, v) }
    val resp = client.send(b.build(), HttpResponse.BodyHandlers.ofInputStream())
    if (resp.statusCode() / 100 != 2) { Try(resp.body().close()); throw new RuntimeException(s"httpSource ${resp.statusCode()}: $url") }
    resp.body()
  }

  def csvRows(url: String, spec: Map[String, Any], headers: Map[String, String], timeout: Int): Iterator[WRow] = {
    val lower   = url.toLowerCase.takeWhile(_ != '?')
    val unzip   = boolOf(spec, "unzip").getOrElse(lower.endsWith(".zip"))
    val gunzip  = boolOf(spec, "gzip").getOrElse(lower.endsWith(".gz"))
    val delim   = spec.get("delimiter").map(_.toString).filter(_.nonEmpty).map(d => if (d == "\\t") '\t' else d.charAt(0)).getOrElse(',')
    val charset = Try(Charset.forName(spec.get("encoding").map(_.toString).getOrElse("UTF-8"))).getOrElse(Charset.forName("UTF-8"))
    val wanted  = asList(spec.getOrElse("columns", null)).map(_.trim).filter(_.nonEmpty)
    val filters = asStrMap(spec.getOrElse("filter", null)).toVector.flatMap { case (k, rx) => Try(k -> rx.r).toOption }
    val limit   = intOf(spec, "limit", 0)
    val srcCol  = spec.get("sourceColumn").map(_.toString.trim).filter(_.nonEmpty)

    val raw = Try(open(url, headers, timeout)).getOrElse(null)
    if (raw == null) return Iterator.empty
    val in: InputStream =
      if (unzip) {
        val z = new ZipInputStream(raw)
        var e = z.getNextEntry
        while (e != null && (e.isDirectory || !e.getName.toLowerCase.matches(".*\\.(csv|txt|tsv)$"))) e = z.getNextEntry
        if (e == null) { Try(z.close()); return Iterator.empty }
        z
      } else if (gunzip) new GZIPInputStream(raw) else raw
    val reader = new CsvReader(new BufferedReader(new InputStreamReader(in, charset), 1 << 16), delim)

    val header = reader.next().map(_.map(h => h.replace("﻿", "").trim)).getOrElse(Array.empty[String])
    if (header.isEmpty) { reader.close(); return Iterator.empty }
    val idx    = header.zipWithIndex.toMap
    val keep: Vector[(String, Int)] =
      if (wanted.nonEmpty) wanted.flatMap(c => idx.get(c).map(c -> _)).toVector else header.zipWithIndex.toVector
    val tests: Vector[(Int, Regex)] = filters.flatMap { case (c, rx) => idx.get(c).map(_ -> rx) }
    // a filter on a column the file does not have keeps nothing: say so with zero rows rather than everything
    if (filters.size != tests.size) { reader.close(); return Iterator.empty }

    def cell(r: Array[String], i: Int): String = if (i < r.length) r(i) else ""
    val rows = Iterator.continually(reader.next()).takeWhile(_.isDefined).map(_.get)
      .filter(r => tests.forall { case (i, rx) => rx.findFirstIn(cell(r, i)).isDefined })
      .map { r =>
        val base = keep.foldLeft(WRow.empty) { case (row, (nm, i)) => row.set(nm, cell(r, i)) }
        srcCol.fold(base)(c => base.set(c, url))
      }
    val limited = if (limit > 0) rows.take(limit) else rows
    new Iterator[WRow] {
      private var alive = true
      def hasNext: Boolean = alive && { val h = limited.hasNext; if (!h) { alive = false; reader.close() }; h }
      def next(): WRow = limited.next()
    }
  }

  /** Minimal RFC-4180 reader: quoted fields, doubled quotes, delimiters and newlines inside quotes. */
  final class CsvReader(r: Reader, delim: Char) {
    private val sb = new java.lang.StringBuilder
    private var eof = false
    private var back: Int = -2                       // one char of push-back (-2 = none)
    private def read(): Int = if (back != -2) { val c = back; back = -2; c } else r.read()
    def close(): Unit = Try(r.close())
    def next(): Option[Array[String]] = {
      if (eof) return None
      val out = scala.collection.mutable.ArrayBuffer.empty[String]
      sb.setLength(0)
      var inQ = false; var any = false
      var c = read()
      while (c != -1) {
        any = true
        val ch = c.toChar
        if (inQ) {
          if (ch == '"') {
            val n = read()
            if (n == '"') sb.append('"')            // escaped quote
            else { inQ = false; back = n }          // closing quote: re-read what follows
          } else sb.append(ch)
        } else if (ch == '"' && sb.length == 0) inQ = true
        else if (ch == delim) { out += sb.toString; sb.setLength(0) }
        else if (ch == '\n') { out += sb.toString; return Some(out.toArray) }
        else if (ch != '\r') sb.append(ch)
        c = read()
      }
      eof = true
      if (!any) None else { out += sb.toString; Some(out.toArray) }
    }
  }
}
