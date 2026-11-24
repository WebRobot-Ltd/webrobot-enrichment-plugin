package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.collection.mutable
import scala.util.Try

/**
 * Flies the routes planned by fleetPlan with the fleet's drones — one output row per acquisition point.
 *
 * Each input row is one drone's route (after `repartition` to the number of drones, each route is one Spark task):
 * the task BORROWS the drone with a lease on the fleet service (no other task can command it meanwhile), connects to
 * its MAVProxy, takes off to the drone's altitude layer, visits the points in order recording position, altitude,
 * heading, speed, battery and the photo footprint at each one, returns to the base and releases the drone.
 *
 * Pipeline YAML:
 * {{{
 * - stage: fleetFly
 *   args:
 *     - 12      # cruise speed, m/s (default 12)
 *     - 70      # camera horizontal field of view, degrees (default 70)
 *     - 55      # camera vertical field of view, degrees (default 55)
 * }}}
 * Adds, per acquisition: uav_point_id, uav_seq, uav_lat, uav_lon, uav_alt_m, uav_heading, uav_groundspeed,
 * uav_battery_pct, uav_sim_time_s, uav_footprint_wkt, uav_gsd_cm, uav_status, uav_source (keeps uav_mission, uav_drone).
 */
class FleetFlyStage extends WPartitionStage {

  override def name: String = "fleetFly"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val speedMs = args.double(0, 12.0)
    val hfovDeg = args.double(1, 70.0)
    val vfovDeg = args.double(2, 55.0)
    rows.flatMap { row =>
      val fleetUrl = String.valueOf(row.fields.getOrElse("uav_fleet", ""))
      val drone = row.double("uav_drone").map(_.toInt).getOrElse(-1)
      val alt = row.double("uav_alt_m").getOrElse(50.0)
      val route = String.valueOf(row.fields.getOrElse("uav_route", "")).split(";").toList.filter(_.contains("@")).flatMap { s =>
        val Array(id, ll) = s.split("@", 2); val Array(a, b) = ll.split(",", 2)
        Try((id, a.toDouble, b.toDouble)).toOption
      }
      val p = DroneSurveyStage.Plan(radius = 0, drones = 1, alt = math.max(20.0, math.min(120.0, alt)),
        speed = math.max(3.0, math.min(20.0, speedMs)), hfov = hfovDeg, vfov = vfovDeg, speedup = 1)
      val out: Seq[Map[String, Any]] =
        if (fleetUrl.isEmpty || drone < 0 || route.isEmpty) Seq(DroneSurveyStage.status("no_route"))
        else Try(Fleet.flyRoute(fleetUrl, drone, route, p, ctx)).recover { case e: Throwable =>
          ctx.warn(s"fleetFly drone $drone: ${e.getMessage}")
          Seq(DroneSurveyStage.status("error:" + Option(e.getMessage).getOrElse(e.getClass.getSimpleName).take(120)))
        }.get
      out.map(cols => Enrichers.augment(WRow(Map("uav_mission" -> row.fields.getOrElse("uav_mission", ""), "uav_drone" -> drone)), cols))
    }
  }
}

