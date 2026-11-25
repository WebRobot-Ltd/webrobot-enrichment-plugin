package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.util.Try

/**
 * Acquires the road network of an area from the traffic simulator service (sumo-sim: Eclipse SUMO, headless) — one
 * output row per road edge.
 *
 * Each input row is an area (name + bounding box, max 16 km²). The service builds the SUMO network from OpenStreetMap
 * (roads open to cars, guessed traffic lights) and returns every edge with its street name, type, length, speed limit,
 * lanes and middle point. `cell_lat`/`cell_lon` snap the middle point to a square grid (`grid_m`, default 100 m), so that
 * a point lookup such as floodLandslideHazard is called once per cell instead of once per edge: a coarser grid means
 * fewer calls (≈ area / grid²) and a coarser hazard map — give the lookup a radius of about half the grid.
 *
 * Pipeline YAML:
 * {{{
 * - stage: trafficNetwork
 *   args:
 *     - "http://sumo-sim.webrobot.svc.cluster.local:8080"   # simulator service
 *     - "area"    # area name column
 *     - "south"   # bbox columns (WGS84 degrees)
 *     - "west"
 *     - "north"
 *     - "east"
 *     - 100       # grid for cell_lat/cell_lon, metres (20-1000, default 100)
 * }}}
 * Output: tr_service, tr_network, tr_edge_id, tr_street, tr_road_type, lat, lon, cell_lat, cell_lon, tr_length_m,
 * tr_speed_kmh, tr_lanes, tr_status, tr_source.
 */
class TrafficNetworkStage extends WPartitionStage {

  override def name: String = "trafficNetwork"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val serviceUrl = args.string(0, "http://sumo-sim.webrobot.svc.cluster.local:8080").stripSuffix("/")
    val areaF  = args.string(1, "area")
    val southF = args.string(2, "south")
    val westF  = args.string(3, "west")
    val northF = args.string(4, "north")
    val eastF  = args.string(5, "east")
    val gridM  = args.double(6, 100.0)
    val grid = math.max(20.0, math.min(1000.0, gridM))
    rows.flatMap { row =>
      val area = String.valueOf(row.fields.getOrElse(areaF, "area"))
      val bbox = Seq(southF, westF, northF, eastF).map(f => row.double(f).getOrElse(Double.NaN))
      if (bbox.exists(_.isNaN)) Iterator(WRow(Map("tr_network" -> area, "tr_status" -> "no_bbox")))
      else Try(TrafficSim.network(serviceUrl, area, bbox, grid)).recover { case e: Throwable =>
        ctx.warn(s"trafficNetwork $area: ${e.getMessage}")
        Seq(WRow(Map[String, Any]("tr_network" -> area, "tr_status" -> ("error:" + String.valueOf(e.getMessage).take(150)))))
      }.get.iterator
    }
  }
}

object TrafficSim {
  import SatelliteIndicesStage.Json

  val DEFAULT_URL = "http://sumo-sim.webrobot.svc.cluster.local:8080"

  private lazy val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()

  def esc(s: String): String = s.replace("\\", "\\\\").replace("\"", "\\\"")

  /** POST with a long timeout (a simulation queues behind the others on the service); non-200 is an error. */
  def post(url: String, body: String, timeoutS: Int): Map[String, Any] = {
    val r = http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(timeoutS.toLong))
      .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(),
      HttpResponse.BodyHandlers.ofString())
    val o = Try(Json.obj(Json.parse(r.body()))).getOrElse(Map.empty[String, Any])
    if (r.statusCode() != 200) throw new RuntimeException(s"HTTP ${r.statusCode()}: " + o.get("error").map(String.valueOf).getOrElse(r.body().take(200)))
    o
  }

  def str(o: Map[String, Any], k: String): String = o.get(k).map(String.valueOf).filter(_ != "null").getOrElse("")

  def network(url: String, area: String, bbox: Seq[Double], gridM: Double): Seq[WRow] = {
    val Seq(s, w, n, e) = bbox
    val o = post(s"$url/api/network", s"""{"name":"${esc(area)}","south":$s,"west":$w,"north":$n,"east":$e}""", 900)
    val net = str(o, "network")
    val source = str(o, "source")
    // grid in degrees: same size in metres on both axes at the area's latitude
    val stepLat = gridM / 111320.0
    val stepLon = gridM / (111320.0 * math.cos(math.toRadians((s + n) / 2)))
    def snap(v: Double, step: Double): Double = math.round(math.round(v / step) * step * 1e6) / 1e6
    Json.arr(o.get("edge_list")).map { x0 =>
      val x = Json.obj(x0)
      val lat = Json.num(x.get("lat")); val lon = Json.num(x.get("lon"))
      WRow(Map[String, Any](
        "tr_service" -> url, "tr_network" -> net, "tr_edge_id" -> str(x, "edge_id"), "tr_street" -> str(x, "street"),
        "tr_road_type" -> str(x, "road_type"), "lat" -> lat, "lon" -> lon,
        "cell_lat" -> snap(lat, stepLat), "cell_lon" -> snap(lon, stepLon),
        "tr_length_m" -> Json.num(x.get("length_m")), "tr_speed_kmh" -> Json.num(x.get("speed_kmh")).toInt,
        "tr_lanes" -> Json.num(x.get("lanes")).toInt, "tr_status" -> "ok", "tr_source" -> source))
    }.toList
  }
}
