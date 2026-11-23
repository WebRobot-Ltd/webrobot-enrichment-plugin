package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.{ByteBuffer, ByteOrder}
import java.time.{Duration, LocalDate}
import java.util.zip.Inflater
import scala.collection.mutable
import scala.util.Try

/**
 * Sentinel-2 spectral indices around each row's coordinates (vegetation, water, moisture, burn), one output row per cloud-free satellite pass — FREE, no API key.
 *
 * For every input row (an asset with latitude/longitude) the stage searches the Copernicus Sentinel-2 L2A
 * archive (Cloud-Optimized GeoTIFFs on the AWS Registry of Open Data, catalogue Earth Search STAC), reads
 * ONLY the 20 m tiles covering a square of `buffer_m` metres around the point (HTTP range requests, no full
 * image download), masks clouds/shadows/snow with the scene classification layer and computes, per pass:
 * NDVI (vegetation), NDWI (open water), NDMI (moisture / drought stress), NBR (burn), plus the share of
 * water pixels. Rows are multiplied: one per (asset, pass). Licence: Copernicus Sentinel data are free and
 * open — credit "Contains modified Copernicus Sentinel data <year>".
 *
 * Pipeline YAML:
 * {{{
 * - stage: satelliteIndices
 *   args:
 *     - "lat"          # latitude column (WGS84)
 *     - "lon"          # longitude column (WGS84)
 *     - 250            # half side of the square around the point, metres (default 250, max 2000)
 *     - "365d"         # from: YYYY-MM-DD, "<n>d" = n days ago (default "365d")
 *     - "today"        # to: YYYY-MM-DD or "today" (default)
 *     - 40             # max scene cloud cover % (default 40)
 *     - 30             # max passes per asset, most recent first (default 30)
 * }}}
 * Adds sat_date, sat_scene, sat_cloud_pct, sat_valid_pct, sat_pixels, ndvi_mean, ndvi_p10, ndvi_p90,
 * ndwi_mean, ndmi_mean, nbr_mean, water_pct, sat_status, sat_source. A row with no usable pass is kept once
 * with sat_status explaining why (no_scenes, all_cloudy, error:…) and empty index columns.
 */
class SatelliteIndicesStage extends WPartitionStage {

  override def name: String = "satelliteIndices"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val latF     = args.string(0, "lat")
    val lonF     = args.string(1, "lon")
    val bufferMetres = args.double(2, 250.0)
    val fromDate     = args.string(3, "365d")
    val toDate       = args.string(4, "today")
    val maxCloud     = args.double(5, 40.0)
    val maxPasses    = args.int(6, 30)
    val bufferM  = math.max(20.0, math.min(2000.0, bufferMetres))
    val from     = SatelliteIndicesStage.day(fromDate, LocalDate.now().minusDays(365))
    val to       = SatelliteIndicesStage.day(toDate, LocalDate.now())
    val maxScene = math.max(1, maxPasses)
    val reader   = new SatelliteIndicesStage.CogReader()
    rows.flatMap { row =>
      val lat = row.double(latF)
      val lon = row.double(lonF)
      val out: Seq[Map[String, Any]] =
        if (lat.isEmpty || lon.isEmpty || lat.get.isNaN || lon.get.isNaN) Seq(SatelliteIndicesStage.status("no_coordinates"))
        else Try(SatelliteIndicesStage.indices(lat.get, lon.get, bufferM, from, to, maxCloud, maxScene, reader, ctx))
          .recover { case e: Throwable =>
            ctx.warn(s"satelliteIndices ${lat.get},${lon.get}: ${e.getMessage}")
            Seq(SatelliteIndicesStage.status("error:" + Option(e.getMessage).getOrElse(e.getClass.getSimpleName).take(120)))
          }.get
      out.map(cols => Enrichers.augment(row, cols))
    }
  }
}

object SatelliteIndicesStage {

  val STAC       = "https://earth-search.aws.element84.com/v1/search"
  val COLLECTION = "sentinel-2-c1-l2a"
  val SOURCE     = "Copernicus Sentinel-2 L2A (ESA), COG via AWS Open Data / Earth Search"
  val RES        = 20.0
  // asset key -> overview level that is 20 m (10 m bands: first overview; 20 m bands: full resolution)
  val BANDS: Seq[(String, Int)] = Seq("red" -> 1, "green" -> 1, "nir" -> 1, "swir16" -> 0, "swir22" -> 0, "scl" -> 0)
  // Scene classification: 4 vegetation, 5 bare soil, 6 water, 7 unclassified. Everything else (no data,
  // saturated, dark, cloud shadow, clouds, cirrus, snow) is excluded.
  val CLEAR: Set[Int] = Set(4, 5, 6, 7)

  val COLUMNS: Seq[String] = Seq("sat_date", "sat_scene", "sat_cloud_pct", "sat_valid_pct", "sat_pixels", "ndvi_mean",
    "ndvi_p10", "ndvi_p90", "ndwi_mean", "ndmi_mean", "nbr_mean", "water_pct", "sat_status", "sat_source")

  /** "2026-01-31", "today", "<n>d" (n days ago) or "" → ISO date. */
  def day(spec: String, dflt: LocalDate): String = {
    val s = Option(spec).map(_.trim.toLowerCase).getOrElse("")
    if (s.isEmpty) dflt.toString
    else if (s == "today") LocalDate.now().toString
    else if (s.matches("""\d+d""")) LocalDate.now().minusDays(s.dropRight(1).toLong).toString
    else Try(LocalDate.parse(s.take(10)).toString).getOrElse(dflt.toString)
  }

  def status(s: String): Map[String, Any] =
    COLUMNS.map(c => c -> (if (c == "sat_status") s else if (c == "sat_source") SOURCE else "")).toMap

  final case class Scene(id: String, date: String, cloud: Double, epsg: Int, originX: Double, originY: Double,
                         hrefs: Map[String, String], scale: Double, offset: Double)

  def indices(lat: Double, lon: Double, bufferM: Double, from: String, to: String, maxCloud: Double, maxScene: Int,
              reader: CogReader, ctx: WebroStageContext): Seq[Map[String, Any]] = {
    val scenes = search(lat, lon, from, to, maxCloud, maxScene * 2, ctx)
    // a point on the overlap of two MGRS tiles comes back twice per pass: measure both, keep the one that
    // actually sees the asset (most clear pixels) — scene-wide cloud cover says nothing about the point
    val dates = scenes.map(_.date).distinct.sorted(Ordering[String].reverse).take(maxScene).toSet
    val todo = scenes.filter(s => dates.contains(s.date))
    if (todo.isEmpty) return Seq(status("no_scenes"))
    val measured = todo.flatMap { s => Try(measure(s, lat, lon, bufferM, reader)).recover { case e: Throwable =>
      ctx.warn(s"satelliteIndices ${s.id}: ${e.getMessage}"); Map.empty[String, Any] }.toOption.filter(_.nonEmpty) }
    def px(m: Map[String, Any]): Int = Try(m.getOrElse("sat_pixels", 0).toString.toInt).getOrElse(0)
    val rows = measured.groupBy(_.getOrElse("sat_date", "")).values.map(_.maxBy(px)).toSeq
      .sortBy(_.getOrElse("sat_date", "").toString)(Ordering[String].reverse)
    val usable = rows.filter(_.get("sat_status").contains("ok"))
    if (usable.nonEmpty) usable
    else if (rows.nonEmpty) Seq(status("all_cloudy"))
    else Seq(status("error:read_failed"))
  }

  // ── STAC search ─────────────────────────────────────────────────────────────────────────────

  def search(lat: Double, lon: Double, from: String, to: String, maxCloud: Double, limit: Int,
             ctx: WebroStageContext): Seq[Scene] = {
    val body =
      s"""{"collections":["$COLLECTION"],"intersects":{"type":"Point","coordinates":[$lon,$lat]},""" +
      s""""datetime":"${from}T00:00:00Z/${to}T23:59:59Z","limit":${math.min(limit, 100)},""" +
      s""""query":{"eo:cloud_cover":{"lt":$maxCloud}},"sortby":[{"field":"properties.datetime","direction":"desc"}]}"""
    val txt = ctx.httpPost(STAC, body, Map("Content-Type" -> "application/json", "Accept" -> "application/geo+json"), 60000)
    val root = Json.parse(txt)
    Json.arr(Json.obj(root).get("features")).flatMap { f0 =>
      val f = Json.obj(f0); val p = Json.obj(f.get("properties")); val assets = Json.obj(f.get("assets"))
      val epsg = Try(String.valueOf(p.getOrElse("proj:code", p.getOrElse("proj:epsg", ""))).split(":").last.toDouble.toInt).getOrElse(0)
      val hrefs = BANDS.flatMap { case (k, _) => Json.obj(assets.get(k)).get("href").map(h => k -> String.valueOf(h)) }.toMap
      val tr = Json.arr(Json.obj(assets.get("scl")).get("proj:transform")).map(Json.num)
      val rb = Json.arr(Json.obj(assets.get("red")).get("raster:bands")).headOption.map(Json.obj).getOrElse(Map.empty)
      if (epsg == 0 || hrefs.size != BANDS.size || tr.size < 6) None
      else Some(Scene(String.valueOf(f.getOrElse("id", "")), String.valueOf(p.getOrElse("datetime", "")).take(10),
        Json.num(p.getOrElse("eo:cloud_cover", 0.0)), epsg, tr(2), tr(5), hrefs,
        rb.get("scale").map(Json.num).getOrElse(0.0001), rb.get("offset").map(Json.num).getOrElse(-0.1)))
    }
  }

  // ── per-scene measurement ───────────────────────────────────────────────────────────────────

  def measure(s: Scene, lat: Double, lon: Double, bufferM: Double, reader: CogReader): Map[String, Any] = {
    val zone = s.epsg % 100
    val south = s.epsg / 100 == 327
    if (!(s.epsg / 100 == 326 || south)) throw new IllegalStateException(s"unsupported EPSG ${s.epsg}")
    val (x, y) = Utm.forward(lat, lon, zone, south)
    val c0 = math.floor((x - bufferM - s.originX) / RES).toInt
    val c1 = math.floor((x + bufferM - s.originX) / RES).toInt
    val r0 = math.floor((s.originY - (y + bufferM)) / RES).toInt
    val r1 = math.floor((s.originY - (y - bufferM)) / RES).toInt
    val w = c1 - c0 + 1; val h = r1 - r0 + 1
    val px = BANDS.map { case (k, lvl) => k -> reader.window(s.hrefs(k), lvl, c0, r0, w, h) }.toMap
    val scl = px("scl"); val red = px("red"); val green = px("green"); val nir = px("nir"); val sw1 = px("swir16"); val sw2 = px("swir22")
    def refl(v: Int): Double = v * s.scale + s.offset
    def nd(a: Double, b: Double): Double = { val d = a + b; if (math.abs(d) < 1e-6) Double.NaN else (a - b) / d }
    val ndvi = mutable.ArrayBuffer.empty[Double]; val ndwi = mutable.ArrayBuffer.empty[Double]
    val ndmi = mutable.ArrayBuffer.empty[Double]; val nbr = mutable.ArrayBuffer.empty[Double]
    var total = 0; var water = 0
    var i = 0
    while (i < w * h) {
      val c = scl(i)
      if (c >= 0) {
        total += 1
        if (CLEAR.contains(c) && red(i) > 0 && nir(i) > 0) {
          val R = refl(red(i)); val G = refl(green(i)); val N = refl(nir(i)); val S1 = refl(sw1(i)); val S2 = refl(sw2(i))
          val wi = nd(G, N)
          // the classifier alone calls dark roofs and shadows "water" in cities: require the spectral test too
          if (c == 6 && wi > 0) water += 1
          ndvi += nd(N, R); ndwi += wi; ndmi += nd(N, S1); nbr += nd(N, S2)
        }
      }
      i += 1
    }
    val valid = ndvi.size
    val validPct = if (total == 0) 0.0 else 100.0 * valid / total
    val base: Map[String, Any] = Map("sat_date" -> s.date, "sat_scene" -> s.id, "sat_cloud_pct" -> r2(s.cloud),
      "sat_valid_pct" -> r2(validPct), "sat_pixels" -> valid, "sat_source" -> SOURCE)
    // under a quarter of the square visible: the pass says nothing reliable about the asset
    if (valid == 0 || validPct < 25.0) status("cloudy") ++ base + ("sat_status" -> "cloudy")
    else base ++ Map(
      "ndvi_mean" -> r3(mean(ndvi)), "ndvi_p10" -> r3(pct(ndvi, 0.10)), "ndvi_p90" -> r3(pct(ndvi, 0.90)),
      "ndwi_mean" -> r3(mean(ndwi)), "ndmi_mean" -> r3(mean(ndmi)), "nbr_mean" -> r3(mean(nbr)),
      "water_pct" -> r2(100.0 * water / valid), "sat_status" -> "ok")
  }

  private def clean(a: scala.collection.Seq[Double]): Seq[Double] = a.filterNot(_.isNaN).toList
  def mean(a: scala.collection.Seq[Double]): Double = { val c = clean(a); if (c.isEmpty) Double.NaN else c.sum / c.size }
  def pct(a: scala.collection.Seq[Double], q: Double): Double = { val c = clean(a).sorted; if (c.isEmpty) Double.NaN else c(math.min(c.size - 1, (q * (c.size - 1)).round.toInt)) }
  private def r2(d: Double): Any = if (d.isNaN) "" else BigDecimal(d).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  private def r3(d: Double): Any = if (d.isNaN) "" else BigDecimal(d).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble

  // ── WGS84 → UTM (Snyder, Transverse Mercator; sub-metre at Sentinel-2 tile scale) ─────────────

  object Utm {
    def forward(lat: Double, lon: Double, zone: Int, south: Boolean): (Double, Double) = {
      val a = 6378137.0; val f = 1 / 298.257223563; val e2 = f * (2 - f); val ep2 = e2 / (1 - e2); val k0 = 0.9996
      val lam0 = math.toRadians(zone * 6 - 183); val phi = math.toRadians(lat); val lam = math.toRadians(lon)
      val sinP = math.sin(phi); val cosP = math.cos(phi); val tanP = math.tan(phi)
      val n = a / math.sqrt(1 - e2 * sinP * sinP); val t = tanP * tanP; val c = ep2 * cosP * cosP; val aa = cosP * (lam - lam0)
      val m = a * ((1 - e2 / 4 - 3 * e2 * e2 / 64 - 5 * e2 * e2 * e2 / 256) * phi
        - (3 * e2 / 8 + 3 * e2 * e2 / 32 + 45 * e2 * e2 * e2 / 1024) * math.sin(2 * phi)
        + (15 * e2 * e2 / 256 + 45 * e2 * e2 * e2 / 1024) * math.sin(4 * phi)
        - (35 * e2 * e2 * e2 / 3072) * math.sin(6 * phi))
      val x = k0 * n * (aa + (1 - t + c) * math.pow(aa, 3) / 6 + (5 - 18 * t + t * t + 72 * c - 58 * ep2) * math.pow(aa, 5) / 120) + 500000.0
      val y = k0 * (m + n * tanP * (aa * aa / 2 + (5 - t + 9 * c + 4 * c * c) * math.pow(aa, 4) / 24
        + (61 - 58 * t + t * t + 600 * c - 330 * ep2) * math.pow(aa, 6) / 720))
      (x, if (south) y + 10000000.0 else y)
    }
  }

  // ── Cloud-Optimized GeoTIFF window reader (tiled, DEFLATE, predictor 1/2, 8/16-bit unsigned) ──

  final class CogReader {
    private final case class Ifd(width: Int, height: Int, tileW: Int, tileH: Int, bits: Int, compression: Int,
                                 predictor: Int, offsets: Array[Long], counts: Array[Long])
    private val headers = mutable.Map.empty[String, (Vector[Ifd], ByteOrder)]
    // decoded tiles, bounded: (url, level, index) -> samples
    private val tiles = new java.util.LinkedHashMap[(String, Int, Int), Array[Int]](64, 0.75f, true) {
      override def removeEldestEntry(e: java.util.Map.Entry[(String, Int, Int), Array[Int]]): Boolean = size() > 96
    }

    /** samples of a w×h window at (col0,row0) of overview `level`; -1 outside the image. */
    def window(url: String, level: Int, col0: Int, row0: Int, w: Int, h: Int): Array[Int] = {
      val (ifds, _) = header(url)
      if (level >= ifds.size) throw new IllegalStateException(s"no overview $level in $url")
      val d = ifds(level)
      val out = Array.fill(w * h)(-1)
      val tilesAcross = (d.width + d.tileW - 1) / d.tileW
      val tr0 = math.max(0, row0) / d.tileH; val tr1 = math.min(d.height - 1, row0 + h - 1) / d.tileH
      val tc0 = math.max(0, col0) / d.tileW; val tc1 = math.min(d.width - 1, col0 + w - 1) / d.tileW
      if (row0 + h - 1 < 0 || col0 + w - 1 < 0 || row0 >= d.height || col0 >= d.width) return out
      for (tr <- tr0 to tr1; tc <- tc0 to tc1) {
        val idx = tr * tilesAcross + tc
        val t = tile(url, level, idx, d)
        val rs = math.max(row0, tr * d.tileH); val re = math.min(math.min(row0 + h, (tr + 1) * d.tileH), d.height)
        val cs = math.max(col0, tc * d.tileW); val ce = math.min(math.min(col0 + w, (tc + 1) * d.tileW), d.width)
        var r = rs
        while (r < re) {
          var c = cs
          while (c < ce) { out((r - row0) * w + (c - col0)) = t((r - tr * d.tileH) * d.tileW + (c - tc * d.tileW)); c += 1 }
          r += 1
        }
      }
      out
    }

    private def tile(url: String, level: Int, idx: Int, d: Ifd): Array[Int] = {
      val key = (url, level, idx)
      val hit = tiles.get(key)
      if (hit != null) return hit
      val n = d.tileW * d.tileH
      val samples = new Array[Int](n)
      if (d.counts(idx) > 0) {
        val raw = Http.range(url, d.offsets(idx), d.counts(idx))
        val bytesPer = d.bits / 8
        val buf = new Array[Byte](n * bytesPer)
        d.compression match {
          case 8 | 32946 =>
            val inf = new Inflater(); inf.setInput(raw)
            var got = 0
            while (got < buf.length && !inf.finished()) { val k = inf.inflate(buf, got, buf.length - got); if (k == 0 && (inf.needsInput() || inf.needsDictionary())) got = buf.length else got += k }
            inf.end()
          case 1 => System.arraycopy(raw, 0, buf, 0, math.min(raw.length, buf.length))
          case other => throw new IllegalStateException(s"unsupported TIFF compression $other")
        }
        val order = header(url)._2
        if (bytesPer == 2) { val sb = ByteBuffer.wrap(buf).order(order).asShortBuffer(); var i = 0; while (i < n) { samples(i) = sb.get(i) & 0xFFFF; i += 1 } }
        else if (bytesPer == 1) { var i = 0; while (i < n) { samples(i) = buf(i) & 0xFF; i += 1 } }
        else throw new IllegalStateException(s"unsupported bits ${d.bits}")
        if (d.predictor == 2) {
          val mask = if (bytesPer == 2) 0xFFFF else 0xFF
          var r = 0
          while (r < d.tileH) { val b = r * d.tileW; var c = 1; while (c < d.tileW) { samples(b + c) = (samples(b + c) + samples(b + c - 1)) & mask; c += 1 }; r += 1 }
        }
      }
      tiles.put(key, samples)
      samples
    }

    private def header(url: String): (Vector[Ifd], ByteOrder) = headers.getOrElseUpdate(url, readHeader(url))

    private def readHeader(url: String): (Vector[Ifd], ByteOrder) = {
      var buf = Http.range(url, 0, 65536)
      def ensure(end: Long): Unit = if (end > buf.length) {
        val more = Http.range(url, buf.length.toLong, end - buf.length + 65536)
        buf = buf ++ more
      }
      val order = if (buf(0) == 'I'.toByte) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN
      def bb = ByteBuffer.wrap(buf).order(order)
      def u16(p: Long): Int = { ensure(p + 2); bb.getShort(p.toInt) & 0xFFFF }
      def u32(p: Long): Long = { ensure(p + 4); bb.getInt(p.toInt) & 0xFFFFFFFFL }
      if (u16(2) != 42) throw new IllegalStateException("BigTIFF / not a TIFF: " + url)
      val out = Vector.newBuilder[Ifd]
      var off = u32(4)
      var guard = 0
      while (off != 0 && guard < 16) {
        val n = u16(off)
        val tags = mutable.Map.empty[Int, Array[Long]]
        for (i <- 0 until n) {
          val p = off + 2 + 12L * i
          val tag = u16(p); val typ = u16(p + 2); val cnt = u32(p + 4)
          val size = typ match { case 3 => 2; case 4 => 4; case 16 => 8; case _ => 0 }
          if (size > 0 && Set(256, 257, 258, 259, 317, 322, 323, 324, 325).contains(tag)) {
            val bytes = size * cnt
            val vo = if (bytes <= 4) p + 8 else u32(p + 8)
            ensure(vo + bytes)
            tags(tag) = Array.tabulate(cnt.toInt) { k => typ match {
              case 3 => u16(vo + 2L * k).toLong
              case 4 => u32(vo + 4L * k)
              case _ => { ensure(vo + 8L * k + 8); bb.getLong((vo + 8L * k).toInt) }
            } }
          }
        }
        def one(t: Int, d: Long): Int = tags.get(t).flatMap(_.headOption).getOrElse(d).toInt
        if (tags.contains(324) && tags.contains(322))
          out += Ifd(one(256, 0), one(257, 0), one(322, 0), one(323, 0), one(258, 8), one(259, 1), one(317, 1), tags(324), tags(325))
        off = u32(off + 2 + 12L * n)
        guard += 1
      }
      (out.result(), order)
    }
  }

  object Http {
    private lazy val client: HttpClient =
      HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).connectTimeout(Duration.ofSeconds(30)).build()

    def range(url: String, from: Long, len: Long): Array[Byte] = {
      var last: Throwable = null
      for (attempt <- 0 until 3) {
        try {
          val req = HttpRequest.newBuilder(URI.create(url)).GET().timeout(Duration.ofSeconds(120))
            .header("Range", s"bytes=$from-${from + len - 1}").header("User-Agent", "WebRobot-ETL/satelliteIndices").build()
          val resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray())
          if (resp.statusCode() == 206 || resp.statusCode() == 200) return resp.body()
          last = new RuntimeException(s"HTTP ${resp.statusCode()} $url")
          if (resp.statusCode() / 100 == 4 && resp.statusCode() != 429) throw last
        } catch { case e: java.io.IOException => last = e }
        Thread.sleep(1000L * (attempt + 1))
      }
      throw last
    }
  }

  // ── minimal JSON reader (the plugin carries no dependencies) ────────────────────────────────

  object Json {
    def obj(v: Any): Map[String, Any] = v match { case Some(x) => obj(x); case m: Map[_, _] => m.asInstanceOf[Map[String, Any]]; case _ => Map.empty }
    def arr(v: Any): Seq[Any] = v match { case Some(x) => arr(x); case s: Seq[_] => s; case _ => Seq.empty }
    def num(v: Any): Double = v match { case Some(x) => num(x); case d: Double => d; case n: Number => n.doubleValue(); case s: String => Try(s.toDouble).getOrElse(Double.NaN); case _ => Double.NaN }

    def parse(s: String): Any = { val p = new P(s); p.ws(); p.value() }

    private final class P(s: String) {
      private var i = 0
      def ws(): Unit = while (i < s.length && Character.isWhitespace(s.charAt(i))) i += 1
      def value(): Any = {
        ws()
        if (i >= s.length) throw new IllegalArgumentException("unexpected end of JSON")
        s.charAt(i) match {
          case '{' => i += 1; val m = mutable.LinkedHashMap.empty[String, Any]; ws()
            if (s.charAt(i) == '}') { i += 1 } else {
              var more = true
              while (more) { ws(); val k = str(); ws(); i += 1; m(k) = value(); ws(); if (s.charAt(i) == ',') i += 1 else { i += 1; more = false } }
            }
            m.toMap
          case '[' => i += 1; val b = Vector.newBuilder[Any]; ws()
            if (s.charAt(i) == ']') { i += 1 } else {
              var more = true
              while (more) { b += value(); ws(); if (s.charAt(i) == ',') i += 1 else { i += 1; more = false } }
            }
            b.result()
          case '"' => str()
          case 't' => i += 4; true
          case 'f' => i += 5; false
          case 'n' => i += 4; null
          case _ => val st = i; while (i < s.length && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) i += 1; s.substring(st, i).toDouble
        }
      }
      def str(): String = {
        i += 1; val sb = new StringBuilder
        while (s.charAt(i) != '"') {
          val c = s.charAt(i)
          if (c == '\\') {
            s.charAt(i + 1) match {
              case 'n' => sb.append('\n'); case 't' => sb.append('\t'); case 'r' => sb.append('\r'); case 'b' => sb.append('\b'); case 'f' => sb.append('\f')
              case 'u' => sb.append(Integer.parseInt(s.substring(i + 2, i + 6), 16).toChar); i += 4
              case o => sb.append(o)
            }
            i += 2
          } else { sb.append(c); i += 1 }
        }
        i += 1; sb.toString
      }
    }
  }
}
