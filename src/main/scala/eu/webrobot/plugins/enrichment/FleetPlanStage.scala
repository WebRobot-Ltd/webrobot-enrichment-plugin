package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.collection.mutable
import scala.util.Try

/**
 * Plans an inspection mission for a persistent drone FLEET (the uav-fleet service: simulated ArduPilot copters, one
 * MAVProxy each) — one output row per drone, carrying its route.
 *
 * Reads the fleet (base and drones) from the service, then solves the routing of ALL the partition's points across
 * the drones with a vehicle-routing heuristic: angular sweep around the base into balanced sectors, nearest neighbour
 * inside each sector, improved by 2-opt. Each drone flies at its own altitude layer, so routes never conflict. The
 * plan is published to the fleet's live map. Put all the points in one partition first (`repartition` 1) for one
 * global plan; then `repartition` to the number of drones so that each drone becomes one Spark task (stage fleetFly).
 *
 * Pipeline YAML:
 * {{{
 * - stage: fleetPlan
 *   args:
 *     - "http://uav-fleet.webrobot.svc.cluster.local:8080"   # fleet service
 *     - "lat"          # latitude column of the point to inspect
 *     - "lon"          # longitude column
 *     - "point_id"     # id column of the point (optional)
 *     - 50             # altitude of the lowest layer, metres (default 50)
 *     - 10             # vertical separation between drones, metres (default 10)
 *     - "inspection"   # mission name (default "inspection")
 * }}}
 * Output, one row per drone used: uav_mission, uav_fleet, uav_drone, uav_alt_m, uav_points, uav_route_km, uav_route
 * ("id@lat,lon;…" in flying order).
 */
class FleetPlanStage extends WPartitionStage {

  override def name: String = "fleetPlan"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val fleetUrl   = args.string(0, "http://uav-fleet.webrobot.svc.cluster.local:8080").stripSuffix("/")
    val latF       = args.string(1, "lat")
    val lonF       = args.string(2, "lon")
    val idF        = args.string(3, "")
    val baseAltM   = args.double(4, 50.0)
    val layerM     = args.double(5, 10.0)
    val missionName = args.string(6, "inspection")
    val pts = rows.toList.flatMap { r =>
      for (a <- r.double(latF); b <- r.double(lonF) if !a.isNaN && !b.isNaN)
        yield (if (idF.nonEmpty) String.valueOf(r.fields.getOrElse(idF, "")) else f"$a%.5f,$b%.5f", a, b)
    }
    if (pts.isEmpty) return Iterator.empty
    val fleet = Fleet.read(fleetUrl, ctx)
    val k = math.min(fleet.drones.size, pts.size)
    val routes = Fleet.sweep(fleet.base, pts, k).map(Fleet.twoOpt(fleet.base, _))
    val mission = s"$missionName · ${pts.size} points · $k drones"
    val plan = routes.zipWithIndex.flatMap { case (rt, i) => rt.zipWithIndex.map { case ((id, a, b), n) =>
      s"""{"id":"${Fleet.esc(id)}","lat":$a,"lon":$b,"drone":${fleet.drones(i)},"seq":$n}""" } }
    Try(ctx.httpPost(s"$fleetUrl/api/plan", s"""{"mission":"${Fleet.esc(mission)}","points":[${plan.mkString(",")}]}""",
      Map("Content-Type" -> "application/json"), 15000)).failed.foreach(e => ctx.warn(s"fleetPlan: map not updated: ${e.getMessage}"))
    routes.zipWithIndex.filter(_._1.nonEmpty).map { case (rt, i) =>
      WRow(Map[String, Any](
        "uav_mission" -> mission, "uav_fleet" -> fleetUrl, "uav_drone" -> fleet.drones(i),
        "uav_alt_m" -> (baseAltM + i * layerM), "uav_points" -> rt.size,
        "uav_route_km" -> math.round(Fleet.length(fleet.base, rt) / 10.0) / 100.0,
        "uav_route" -> rt.map { case (id, a, b) => s"${id.replace("@", "_").replace(";", "_")}@$a,$b" }.mkString(";")))
    }.iterator
  }
}

object Fleet {
  import DroneSurveyStage.{dist, footprintWkt, Mav, SENSOR_PX}
  import SatelliteIndicesStage.Json

  final case class FleetInfo(base: (Double, Double), drones: Seq[Int])

  def esc(s: String): String = s.replace("\\", "\\\\").replace("\"", "\\\"")

  private lazy val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()

  def post(url: String, body: String): (Int, String) = {
    val r = http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(15)).header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString())
    (r.statusCode(), r.body())
  }

  def read(url: String, ctx: WebroStageContext): FleetInfo = {
    val o = Json.obj(Json.parse(ctx.httpGet(s"$url/api/fleet", Map.empty, 15000)))
    val base = Json.arr(o.get("base")).map(Json.num)
    val drones = Json.arr(o.get("drones")).map(d => Json.num(Json.obj(d).get("id")).toInt).sorted
    if (base.size < 2 || drones.isEmpty) throw new RuntimeException("fleet service: no base or no drones")
    FleetInfo((base(0), base(1)), drones)
  }

  type Pt = (String, Double, Double)

  /** Sweep heuristic: points sorted by bearing around the base, cut into k sectors with about the same route cost. */
  def sweep(base: (Double, Double), pts: Seq[Pt], k: Int): Seq[Seq[Pt]] = {
    def bearing(p: Pt): Double = math.atan2(p._3 - base._2, p._2 - base._1)
    val sorted = pts.sortBy(bearing)
    // start the sweep at the widest angular gap, so that no sector straddles two far-apart groups
    val angles = sorted.map(bearing)
    val gaps = angles.indices.map(i => ((if (i + 1 < angles.size) angles(i + 1) else angles.head + 2 * math.Pi) - angles(i), i))
    val start = (gaps.maxBy(_._1)._2 + 1) % sorted.size
    val ring = sorted.drop(start) ++ sorted.take(start)
    val w = ring.map(p => dist(base, (p._2, p._3)) + 50.0)   // cost ~ distance from the base + per-point overhead
    val total = w.sum
    val out = mutable.ArrayBuffer.fill(k)(mutable.ArrayBuffer[Pt]())
    var g = 0; var acc = 0.0
    ring.zip(w).foreach { case (p, c) =>
      if (g < k - 1 && acc > 0 && acc + c / 2 > total * (g + 1) / k) g += 1
      out(g) += p; acc += c
    }
    out.map(_.toList).toList
  }

  def length(base: (Double, Double), rt: Seq[Pt]): Double = {
    val path = Seq(base) ++ rt.map(p => (p._2, p._3)) ++ Seq(base)
    path.zip(path.drop(1)).map { case (a, b) => dist(a, b) }.sum
  }

  /** Nearest neighbour from the base, then 2-opt until no improvement (closed tour base → points → base). */
  def twoOpt(base: (Double, Double), pts: Seq[Pt]): Seq[Pt] = {
    if (pts.size < 3) return pts
    val left = mutable.ArrayBuffer(pts: _*); val nn = mutable.ArrayBuffer[Pt]()
    var cur = base
    while (left.nonEmpty) { val n = left.minBy(p => dist(cur, (p._2, p._3))); nn += n; left -= n; cur = (n._2, n._3) }
    var best = nn.toList; var bestLen = length(base, best); var improved = true; var rounds = 0
    while (improved && rounds < 50) {
      improved = false; rounds += 1
      for (i <- 0 until best.size - 1; j <- i + 1 until best.size) {
        val cand = best.take(i) ++ best.slice(i, j + 1).reverse ++ best.drop(j + 1)
        val l = length(base, cand)
        if (l + 0.01 < bestLen) { best = cand; bestLen = l; improved = true }
      }
    }
    best
  }

  /** Borrows the drone (lease), flies the route, releases it. */
  def flyRoute(fleetUrl: String, drone: Int, route: Seq[Pt], p: DroneSurveyStage.Plan, ctx: WebroStageContext): Seq[Map[String, Any]] = {
    val holder = s"spark-${java.util.UUID.randomUUID().toString.take(8)}"
    val host = URI.create(fleetUrl).getHost
    var port = -1; val leaseUntil = System.currentTimeMillis() + 15 * 60 * 1000L
    while (port < 0) {
      val (code, body) = post(s"$fleetUrl/api/lease", s"""{"drone":$drone,"holder":"$holder","ttl_s":3600}""")
      if (code == 200) port = Json.num(Json.obj(Json.parse(body)).get("port")).toInt
      else if (code == 409 && System.currentTimeMillis() < leaseUntil) Thread.sleep(5000)
      else throw new RuntimeException(s"drone $drone not available (lease HTTP $code)")
    }
    val acqs = mutable.ArrayBuffer[Map[String, Any]]()
    val m = new Mav(port, host)
    try {
      if (m.recv(Set(0), 30000).isEmpty) throw new RuntimeException("no heartbeat from the drone")
      m.streams(10)
      var guided = false; var k = 0
      while (!guided && k < 30) { m.setMode(4); m.recv(Set(0), 3000).foreach { case (_, b) => guided = b.getInt(0) == 4 }; k += 1 }
      if (!guided) throw new RuntimeException("GUIDED refused")
      var armed = false; k = 0
      while (!armed && k < 40) { m.command(400, Seq(1f)); armed = m.awaitAck(400, 2000).contains(0); if (!armed) Thread.sleep(1000); k += 1 }
      if (!armed) throw new RuntimeException("arming refused")
      m.command(22, Seq(0f, 0f, 0f, 0f, 0f, 0f, p.alt.toFloat))
      if (!m.awaitAck(22, 10000).contains(0)) throw new RuntimeException("takeoff refused")
      var up = false; val climb = System.currentTimeMillis() + 180000
      while (!up && System.currentTimeMillis() < climb) m.recv(Set(33), 5000).foreach { case (_, b) => up = b.getInt(16) / 1000.0 >= p.alt * 0.95 }
      if (!up) throw new RuntimeException("takeoff did not reach altitude")
      m.command(178, Seq(1f, p.speed.toFloat, -1f))
      var battery = -1; var speed = 0.0
      route.zipWithIndex.foreach { case ((id, lat, lon), n) =>
        m.goto(lat, lon, p.alt)
        var done = false; var last: Option[java.nio.ByteBuffer] = None
        val limit = System.currentTimeMillis() + 180000
        while (!done && System.currentTimeMillis() < limit) m.recv(Set(33, 1, 74), 5000).foreach {
          case (33, b) => last = Some(b); done = dist((b.getInt(4) / 1e7, b.getInt(8) / 1e7), (lat, lon)) < 3.0
          case (1, b)  => battery = b.get(30).toInt
          case (74, b) => speed = b.getFloat(4).toDouble
          case _ =>
        }
        last.foreach { b =>
          val (la, lo) = (b.getInt(4) / 1e7, b.getInt(8) / 1e7)
          acqs += Map[String, Any]("uav_point_id" -> id, "uav_seq" -> n, "uav_lat" -> la, "uav_lon" -> lo,
            "uav_alt_m" -> math.round(b.getInt(16) / 100.0) / 10.0, "uav_heading" -> (b.getShort(26) & 0xFFFF) / 100.0,
            "uav_groundspeed" -> math.round(speed * 10) / 10.0, "uav_battery_pct" -> battery,
            "uav_sim_time_s" -> math.round(b.getInt(0) / 100.0) / 10.0,
            "uav_footprint_wkt" -> (if (done) footprintWkt(la, lo, p) else ""),
            "uav_gsd_cm" -> math.round(p.footW / SENSOR_PX * 1000) / 10.0,
            "uav_status" -> (if (done) "ok" else "point_not_reached"), "uav_source" -> DroneSurveyStage.SOURCE.replace("SITL Copter", "SITL fleet Copter"))
          Try(post(s"$fleetUrl/api/acquired", s"""{"drone":$drone,"id":"${esc(id)}","lat":$la,"lon":$lo,"ok":$done}"""))
        }
      }
      m.setMode(6)   // RTL: back to the base, land, disarm
      var landed = false; val home = System.currentTimeMillis() + 300000
      while (!landed && System.currentTimeMillis() < home) m.recv(Set(0), 5000).foreach { case (_, b) => landed = (b.get(6) & 0x80) == 0 }
    } finally {
      m.close()
      Try(post(s"$fleetUrl/api/release", s"""{"drone":$drone,"holder":"$holder"}"""))
    }
    acqs.toList
  }
}
