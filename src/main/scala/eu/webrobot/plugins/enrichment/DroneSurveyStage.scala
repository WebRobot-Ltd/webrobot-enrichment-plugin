package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import java.io.{File, InputStream, OutputStream}
import java.net.{InetSocketAddress, Socket, SocketTimeoutException, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, StandardCopyOption}
import java.security.MessageDigest
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.util.Try

/**
 * Drone-fleet survey around each row's coordinates, flown by SIMULATED copters (ArduPilot SITL) — one output row
 * per acquisition point.
 *
 * For every input row (a site with latitude/longitude) a small fleet takes off from a pad just south of the area,
 * surveys the disc of `radius_m` metres on a grid of photo points derived from the altitude and the camera field of
 * view, split among the drones in parallel strips (boustrophedon, no crossings, balanced by number of points), and
 * returns to land. At each point the stage records the drone's telemetry (position, altitude, heading, ground
 * speed, battery, simulated time) and the photo footprint on the ground. The flight is real autopilot behaviour —
 * ArduCopter in software-in-the-loop, spoken to over MAVLink 2 — but the camera is not simulated: an acquisition is
 * the position and footprint where a photo would be taken.
 *
 * The simulator (ArduPilot Copter-4.7.3, statically linked, GPLv3, source: github.com/ArduPilot/ardupilot tag
 * Copter-4.7.3) is downloaded once per JVM and checked against its sha256. Each partition flies its own fleets, so
 * sites in different partitions are surveyed in parallel on different executors.
 *
 * Pipeline YAML:
 * {{{
 * - stage: droneSurvey
 *   args:
 *     - "lat"        # latitude column of the site (WGS84)
 *     - "lon"        # longitude column of the site (WGS84)
 *     - 300          # radius of the surveyed area, metres (default 300, 50-1000)
 *     - 3            # drones per site (default 3, 1-6)
 *     - 80           # flight altitude above take-off, metres (default 80, 20-120)
 *     - 12           # cruise speed, m/s (default 12, 3-20)
 *     - 70           # camera horizontal field of view, degrees (default 70)
 *     - 55           # camera vertical field of view, degrees (default 55)
 *     - 10           # simulation speed-up (default 10, 1-20)
 *     - "asset_id"   # optional id column, used as mission name
 * }}}
 * Adds uav_mission, uav_drone, uav_point, uav_lat, uav_lon, uav_alt_m, uav_heading, uav_groundspeed, uav_battery_pct,
 * uav_sim_time_s, uav_footprint_wkt, uav_gsd_cm, uav_status, uav_source.
 */
class DroneSurveyStage extends WPartitionStage {

  override def name: String = "droneSurvey"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val latF           = args.string(0, "lat")
    val lonF           = args.string(1, "lon")
    val radiusMetres   = args.double(2, 300.0)
    val dronesPerSite  = args.int(3, 3)
    val altitudeMetres = args.double(4, 80.0)
    val speedMs        = args.double(5, 12.0)
    val hfovDeg        = args.double(6, 70.0)
    val vfovDeg        = args.double(7, 55.0)
    val simSpeedup     = args.int(8, 10)
    val idF            = args.string(9, "")
    val p = DroneSurveyStage.Plan(
      radius  = math.max(50.0, math.min(1000.0, radiusMetres)),
      drones  = math.max(1, math.min(6, dronesPerSite)),
      alt     = math.max(20.0, math.min(120.0, altitudeMetres)),
      speed   = math.max(3.0, math.min(20.0, speedMs)),
      hfov    = math.max(20.0, math.min(120.0, hfovDeg)),
      vfov    = math.max(15.0, math.min(100.0, vfovDeg)),
      speedup = math.max(1, math.min(20, simSpeedup)))
    rows.flatMap { row =>
      val lat = row.double(latF)
      val lon = row.double(lonF)
      val mission = (if (idF.nonEmpty) Option(row.fields.getOrElse(idF, null)).map(String.valueOf) else None)
        .filter(_.nonEmpty).getOrElse(lat.map(a => f"$a%.5f").getOrElse("") + "," + lon.map(b => f"$b%.5f").getOrElse(""))
      val out: Seq[Map[String, Any]] =
        if (lat.isEmpty || lon.isEmpty || lat.get.isNaN || lon.get.isNaN) Seq(DroneSurveyStage.status("no_coordinates"))
        else Try(DroneSurveyStage.survey(lat.get, lon.get, p, ctx)).recover { case e: Throwable =>
          ctx.warn(s"droneSurvey $mission: ${e.getMessage}")
          Seq(DroneSurveyStage.status("error:" + Option(e.getMessage).getOrElse(e.getClass.getSimpleName).take(120)))
        }.get
      out.map(cols => Enrichers.augment(row, cols + ("uav_mission" -> mission)))
    }
  }
}

object DroneSurveyStage {

  val VERSION = "copter-4.7.3"
  val BASE    = s"https://s3.metaglobe.finance/sparklogs-web/tools/ardupilot/$VERSION/"
  val SOURCE  = "ArduPilot SITL Copter-4.7.3 (simulated flight, MAVLink 2); camera footprint computed, not simulated"
  val EARTH   = 6378137.0
  val SENSOR_PX = 4000.0   // photo width in pixels, for the ground sample distance

  val COLUMNS: Seq[String] = Seq("uav_mission", "uav_drone", "uav_point", "uav_lat", "uav_lon", "uav_alt_m", "uav_heading",
    "uav_groundspeed", "uav_battery_pct", "uav_sim_time_s", "uav_footprint_wkt", "uav_gsd_cm", "uav_status", "uav_source")

  final case class Plan(radius: Double, drones: Int, alt: Double, speed: Double, hfov: Double, vfov: Double, speedup: Int) {
    val footW: Double = 2 * alt * math.tan(math.toRadians(hfov / 2))   // across track (east-west), metres
    val footH: Double = 2 * alt * math.tan(math.toRadians(vfov / 2))   // along track (north-south), metres
  }

  def status(s: String): Map[String, Any] =
    COLUMNS.filter(_ != "uav_mission").map(c => c -> (if (c == "uav_status") s else if (c == "uav_source") SOURCE else "")).toMap

  // ── geometry ──────────────────────────────────────────────────────────────────────────────────────

  def offset(lat: Double, lon: Double, dn: Double, de: Double): (Double, Double) =
    (lat + math.toDegrees(dn / EARTH), lon + math.toDegrees(de / (EARTH * math.cos(math.toRadians(lat)))))

  def dist(a: (Double, Double), b: (Double, Double)): Double = {
    val dn = math.toRadians(b._1 - a._1) * EARTH
    val de = math.toRadians(b._2 - a._2) * EARTH * math.cos(math.toRadians(a._1))
    math.hypot(dn, de)
  }

  /** Photo points (north, east offsets in metres) over the disc, in rows south→north. */
  def grid(p: Plan): Seq[Seq[(Double, Double)]] = {
    val rows = mutable.ArrayBuffer[Seq[(Double, Double)]]()
    var y = -p.radius + p.footH / 2
    while (y <= p.radius) {
      val xs = mutable.ArrayBuffer[(Double, Double)]()
      var x = -p.radius + p.footW / 2
      while (x <= p.radius) { if (math.hypot(x, y) <= p.radius) xs += ((y, x)); x += p.footW }
      if (xs.nonEmpty) rows += xs.toList
      y += p.footH
    }
    rows.toList
  }

  /** Contiguous groups of rows with about the same number of points; each flown boustrophedon. */
  def strips(rows: Seq[Seq[(Double, Double)]], k: Int): Seq[Seq[(Double, Double)]] = {
    val total = rows.map(_.size).sum
    val groups = mutable.ArrayBuffer.fill(k)(mutable.ArrayBuffer[Seq[(Double, Double)]]())
    var g = 0; var acc = 0
    for (r <- rows) {
      if (g < k - 1 && acc > 0 && acc + r.size / 2.0 > total.toDouble * (g + 1) / k) g += 1
      val grp = groups(g)
      grp += (if (grp.size % 2 == 0) r else r.reverse)
      acc += r.size
    }
    groups.map(_.flatten.toList).toList.filter(_.nonEmpty)
  }

  def footprintWkt(lat: Double, lon: Double, p: Plan): String = {
    val c = Seq((-p.footH / 2, -p.footW / 2), (-p.footH / 2, p.footW / 2), (p.footH / 2, p.footW / 2), (p.footH / 2, -p.footW / 2), (-p.footH / 2, -p.footW / 2))
      .map { case (dn, de) => offset(lat, lon, dn, de) }
    c.map { case (a, b) => f"$b%.6f $a%.6f" }.mkString("POLYGON((", ", ", "))")
  }

  // ── simulator ─────────────────────────────────────────────────────────────────────────────────────

  private val instances = new AtomicInteger(0)
  @volatile private var installed: Option[(File, File)] = None

  /** The SITL binary and its parameters, downloaded once per JVM and verified. */
  def install(): (File, File) = synchronized {
    installed.getOrElse {
      val dir = new File(System.getProperty("java.io.tmpdir"), s"webrobot-sitl-$VERSION"); dir.mkdirs()
      val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).followRedirects(HttpClient.Redirect.NORMAL).build()
      def get(name: String): Array[Byte] = {
        val r = http.send(HttpRequest.newBuilder(URI.create(BASE + name)).timeout(Duration.ofSeconds(180)).GET().build(), HttpResponse.BodyHandlers.ofByteArray())
        if (r.statusCode() != 200) throw new RuntimeException(s"simulator download $name: HTTP ${r.statusCode()}")
        r.body()
      }
      val bin = new File(dir, "arducopter"); val parm = new File(dir, "copter.parm")
      val want = new String(get("arducopter.sha256"), "UTF-8").trim.take(64)
      def sha(b: Array[Byte]) = MessageDigest.getInstance("SHA-256").digest(b).map(x => f"${x & 0xff}%02x").mkString
      if (!bin.exists() || sha(Files.readAllBytes(bin.toPath)) != want) {
        val b = get("arducopter")
        if (sha(b) != want) throw new RuntimeException("simulator checksum mismatch")
        val tmp = new File(dir, "arducopter.part"); Files.write(tmp.toPath, b)
        Files.move(tmp.toPath, bin.toPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
      }
      if (!parm.exists()) Files.write(parm.toPath, get("copter.parm"))
      bin.setExecutable(true)
      installed = Some((bin, parm)); (bin, parm)
    }
  }

  // ── MAVLink 2 (hand-written, the ten messages this stage needs) ──────────────────────────────────

  val CRC_EXTRA: Map[Int, Int] = Map(0 -> 50, 1 -> 124, 33 -> 104, 74 -> 20, 253 -> 83, 76 -> 152, 77 -> 143, 11 -> 89, 66 -> 148, 86 -> 5)
  val LEN: Map[Int, Int]       = Map(0 -> 9, 1 -> 43, 33 -> 28, 74 -> 20, 253 -> 54, 76 -> 33, 77 -> 10, 11 -> 6, 66 -> 6, 86 -> 53)

  def crcX25(data: Array[Byte], from: Int, until: Int, seed: Int = 0xFFFF): Int = {
    var crc = seed; var i = from
    while (i < until) {
      var t = (data(i) & 0xFF) ^ (crc & 0xFF)
      t = (t ^ (t << 4)) & 0xFF
      crc = ((crc >> 8) ^ (t << 8) ^ (t << 3) ^ (t >> 4)) & 0xFFFF
      i += 1
    }
    crc
  }

  def le(n: Int): ByteBuffer = ByteBuffer.allocate(n).order(ByteOrder.LITTLE_ENDIAN)

  final class Mav(port: Int, host: String = "127.0.0.1") {
    private val sock = new Socket()
    sock.connect(new InetSocketAddress(host, port), 5000)
    sock.setSoTimeout(500)
    private val in: InputStream = sock.getInputStream
    private val out: OutputStream = sock.getOutputStream
    private val buf = new java.io.ByteArrayOutputStream()
    private var pending: Array[Byte] = Array.emptyByteArray
    private var seq = 0

    def close(): Unit = Try(sock.close())

    def send(msgid: Int, payload: Array[Byte]): Unit = synchronized {
      val frame = new Array[Byte](10 + payload.length + 2)
      frame(0) = 0xFD.toByte; frame(1) = payload.length.toByte; frame(2) = 0; frame(3) = 0
      frame(4) = (seq & 0xFF).toByte; frame(5) = 255.toByte; frame(6) = 190.toByte
      frame(7) = (msgid & 0xFF).toByte; frame(8) = ((msgid >> 8) & 0xFF).toByte; frame(9) = ((msgid >> 16) & 0xFF).toByte
      System.arraycopy(payload, 0, frame, 10, payload.length)
      val withExtra = java.util.Arrays.copyOfRange(frame, 1, 10 + payload.length + 1)
      withExtra(withExtra.length - 1) = CRC_EXTRA(msgid).toByte
      val crc = crcX25(withExtra, 0, withExtra.length)
      frame(10 + payload.length) = (crc & 0xFF).toByte; frame(11 + payload.length) = ((crc >> 8) & 0xFF).toByte
      out.write(frame); out.flush(); seq += 1
    }

    /** Next message among `want` (msgid → payload padded to full length), or None after `timeoutMs`. */
    def recv(want: Set[Int], timeoutMs: Long): Option[(Int, ByteBuffer)] = {
      val end = System.currentTimeMillis() + timeoutMs
      val chunk = new Array[Byte](8192)
      while (System.currentTimeMillis() < end) {
        parse(want) match { case some @ Some(_) => return some; case None => }
        try {
          val n = in.read(chunk)
          if (n < 0) throw new RuntimeException("simulator closed the link")
          pending = pending ++ java.util.Arrays.copyOf(chunk, n)
        } catch { case _: SocketTimeoutException => }
      }
      None
    }

    private def parse(want: Set[Int]): Option[(Int, ByteBuffer)] = {
      var b = pending
      while (true) {
        val i = b.indexWhere(x => x == 0xFD.toByte || x == 0xFE.toByte)
        if (i < 0) { pending = Array.emptyByteArray; return None }
        b = b.drop(i)
        val v2 = b(0) == 0xFD.toByte
        val hdr = if (v2) 10 else 6
        if (b.length < hdr) { pending = b; return None }
        val ln = b(1) & 0xFF
        val total = hdr + ln + 2 + (if (v2 && (b(2) & 1) != 0) 13 else 0)
        if (b.length < total) { pending = b; return None }
        val msgid = if (v2) (b(7) & 0xFF) | ((b(8) & 0xFF) << 8) | ((b(9) & 0xFF) << 16) else b(5) & 0xFF
        val sysid = if (v2) b(5) & 0xFF else b(3) & 0xFF   // only the autopilot (system 1): a router (MAVProxy) also sends heartbeats
        val crcIn = (b(hdr + ln) & 0xFF) | ((b(hdr + ln + 1) & 0xFF) << 8)
        val frame = b.slice(0, total); b = b.drop(total)
        CRC_EXTRA.get(msgid).foreach { extra =>
          val chk = java.util.Arrays.copyOfRange(frame, 1, hdr + ln + 1)
          chk(chk.length - 1) = extra.toByte
          if (crcX25(chk, 0, chk.length) == crcIn && sysid == 1 && want.contains(msgid)) {
            val payload = java.util.Arrays.copyOf(frame.slice(hdr, hdr + ln), math.max(ln, LEN(msgid)))   // MAVLink 2 trims trailing zeros
            pending = b
            return Some((msgid, ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN)))
          }
        }
      }
      None
    }

    /** The acknowledgement of THIS command (a router may repeat older ones): Some(result) or None after `timeoutMs`. */
    def awaitAck(cmd: Int, timeoutMs: Long): Option[Int] = {
      val end = System.currentTimeMillis() + timeoutMs
      while (System.currentTimeMillis() < end) {
        recv(Set(77), math.max(1L, end - System.currentTimeMillis())) match {
          case Some((_, b)) if (b.getShort(0) & 0xFFFF) == cmd => return Some(b.get(2) & 0xFF)
          case _ =>
        }
      }
      None
    }

    def command(cmd: Int, p: Seq[Float]): Unit = {
      val bb = le(33); (0 until 7).foreach(i => bb.putFloat(p.lift(i).getOrElse(0f)))
      bb.putShort(cmd.toShort); bb.put(1.toByte); bb.put(1.toByte); bb.put(0.toByte)
      send(76, bb.array())
    }
    def setMode(custom: Int): Unit = { val bb = le(6); bb.putInt(custom); bb.put(1.toByte); bb.put(1.toByte); send(11, bb.array()) }
    def streams(rate: Int): Unit = { val bb = le(6); bb.putShort(rate.toShort); bb.put(1.toByte); bb.put(1.toByte); bb.put(0.toByte); bb.put(1.toByte); send(66, bb.array()) }
    def goto(lat: Double, lon: Double, alt: Double): Unit = {
      val bb = le(53); bb.putInt(0); bb.putInt(math.round(lat * 1e7).toInt); bb.putInt(math.round(lon * 1e7).toInt); bb.putFloat(alt.toFloat)
      (0 until 8).foreach(_ => bb.putFloat(0f))
      bb.putShort(0x0DF8.toShort); bb.put(1.toByte); bb.put(1.toByte); bb.put(6.toByte)   // position only; GLOBAL_RELATIVE_ALT_INT
      send(86, bb.array())
    }
  }

  // ── one site ──────────────────────────────────────────────────────────────────────────────────────

  final case class Acq(drone: Int, point: Int, lat: Double, lon: Double, alt: Double, heading: Double, speed: Double,
                       battery: Int, simTime: Double, status: String)

  def survey(lat: Double, lon: Double, p: Plan, ctx: WebroStageContext): Seq[Map[String, Any]] = {
    val (bin, parm) = install()
    val plan = strips(grid(p), p.drones)
    val pad = offset(lat, lon, -p.radius - 100, 0)
    val results = java.util.Collections.synchronizedList(new java.util.ArrayList[Acq]())
    val errors = java.util.Collections.synchronizedList(new java.util.ArrayList[String]())
    val procs = mutable.ArrayBuffer[Process]()
    try {
      val threads = plan.zipWithIndex.map { case (pts, i) =>
        val inst = instances.getAndIncrement() % 200
        val home = offset(pad._1, pad._2, 0, (i - (plan.size - 1) / 2.0) * 15)
        val wd = Files.createTempDirectory(s"sitl$inst-").toFile
        val pb = new ProcessBuilder(bin.getAbsolutePath, "--model", "+", "--speedup", p.speedup.toString, "-I", inst.toString,
          "--home", f"${home._1}%.7f,${home._2}%.7f,0,0", "--defaults", parm.getAbsolutePath)
          .directory(wd).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD)
        procs.synchronized { procs += pb.start() }
        val wps = pts.map { case (dn, de) => offset(lat, lon, dn, de) }
        val t = new Thread(() => {
          try fly(i, 5760 + 10 * inst, wps, p, results)
          catch { case e: Throwable => errors.add(s"drone $i: ${Option(e.getMessage).getOrElse(e.getClass.getSimpleName)}") }
        })
        t.setDaemon(true); t.start(); t
      }
      val deadline = System.currentTimeMillis() + 20 * 60 * 1000L
      threads.foreach(t => t.join(math.max(1L, deadline - System.currentTimeMillis())))
    } finally {
      procs.foreach(pr => Try(pr.destroy()))
    }
    val acqs = results.toArray(new Array[Acq](0)).toSeq.sortBy(a => (a.drone, a.point))
    errors.toArray.foreach(e => ctx.warn(s"droneSurvey: $e"))
    if (acqs.isEmpty) Seq(status("error:" + errors.toArray.headOption.map(String.valueOf).getOrElse("no acquisition").take(120)))
    else acqs.map { a =>
      Map[String, Any]("uav_drone" -> a.drone, "uav_point" -> a.point, "uav_lat" -> a.lat, "uav_lon" -> a.lon, "uav_alt_m" -> a.alt,
        "uav_heading" -> a.heading, "uav_groundspeed" -> a.speed, "uav_battery_pct" -> a.battery, "uav_sim_time_s" -> a.simTime,
        "uav_footprint_wkt" -> (if (a.status == "ok") footprintWkt(a.lat, a.lon, p) else ""),
        "uav_gsd_cm" -> math.round(p.footW / SENSOR_PX * 1000) / 10.0, "uav_status" -> a.status, "uav_source" -> SOURCE)
    }
  }

  private def fly(i: Int, port: Int, wps: Seq[(Double, Double)], p: Plan, out: java.util.List[Acq]): Unit = {
    var m: Mav = null
    var tries = 0
    while (m == null) {
      try m = new Mav(port) catch { case e: Throwable => tries += 1; if (tries > 80) throw e; Thread.sleep(250) }
    }
    try {
      if (m.recv(Set(0), 30000).isEmpty) throw new RuntimeException("no heartbeat")
      m.streams(10)
      val ready = System.currentTimeMillis() + 120000
      var gps = false
      while (!gps && System.currentTimeMillis() < ready)
        m.recv(Set(253), 5000).foreach { case (_, b) => gps = new String(b.array(), 1, 50, "US-ASCII").contains("is using GPS") }
      var guided = false; var k = 0
      while (!guided && k < 30) { m.setMode(4); m.recv(Set(0), 3000).foreach { case (_, b) => guided = b.getInt(0) == 4 }; k += 1 }
      if (!guided) throw new RuntimeException("GUIDED refused")
      var armed = false; k = 0
      while (!armed && k < 40) {
        m.command(400, Seq(1f)); armed = m.awaitAck(400, 2000).contains(0)
        if (!armed) Thread.sleep(1000); k += 1
      }
      if (!armed) throw new RuntimeException("arming refused")
      m.command(22, Seq(0f, 0f, 0f, 0f, 0f, 0f, p.alt.toFloat))
      var up = false; val climb = System.currentTimeMillis() + 180000
      while (!up && System.currentTimeMillis() < climb) m.recv(Set(33), 5000).foreach { case (_, b) => up = b.getInt(16) / 1000.0 >= p.alt * 0.95 }
      if (!up) throw new RuntimeException("takeoff did not reach altitude")
      m.command(178, Seq(1f, p.speed.toFloat, -1f))
      var battery = -1; var speed = 0.0
      wps.zipWithIndex.foreach { case (wp, n) =>
        m.goto(wp._1, wp._2, p.alt)
        var done = false; var last: Option[ByteBuffer] = None
        val limit = System.currentTimeMillis() + 120000
        while (!done && System.currentTimeMillis() < limit) m.recv(Set(33, 1, 74), 5000).foreach {
          case (33, b) => last = Some(b); done = dist((b.getInt(4) / 1e7, b.getInt(8) / 1e7), wp) < 3.0
          case (1, b)  => battery = b.get(30).toInt
          case (74, b) => speed = b.getFloat(4).toDouble
          case _ =>
        }
        last.foreach { b =>
          out.add(Acq(i, n, b.getInt(4) / 1e7, b.getInt(8) / 1e7, math.round(b.getInt(16) / 100.0) / 10.0,
            (b.getShort(26) & 0xFFFF) / 100.0, math.round(speed * 10) / 10.0, battery, math.round(b.getInt(0) / 100.0) / 10.0,
            if (done) "ok" else "point_not_reached"))
        }
      }
      m.setMode(6)   // RTL: back to the pad, land, disarm
      var landed = false; val home = System.currentTimeMillis() + 240000
      while (!landed && System.currentTimeMillis() < home) m.recv(Set(0), 5000).foreach { case (_, b) => landed = (b.get(6) & 0x80) == 0 }
    } finally m.close()
  }
}
