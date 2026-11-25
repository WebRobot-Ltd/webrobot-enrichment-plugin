package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.util.Try

/**
 * Runs each scenario (from trafficScenarios) on the traffic simulator service (sumo-sim: Eclipse SUMO, headless) and
 * acquires its impact KPIs — one output row per scenario.
 *
 * The service removes the closed roads, drops the trips whose origin or destination is closed (counted as
 * tr_trips_impossible), simulates the same random origin-destination demand with dynamic rerouting and returns the
 * KPIs plus the most congested streets. The service runs one simulation at a time and queues the others, so
 * `repartition` to the number of scenarios only spreads the waiting, not the load. Simulated traffic, not measured
 * counts: an impact screening to compare scenarios with each other.
 *
 * Pipeline YAML:
 * {{{
 * - stage: trafficSimulate
 *   args:
 *     - 3600   # max wait per scenario, seconds (queue + simulation, default 3600)
 * }}}
 * Adds tr_run_id, tr_trips_planned, tr_trips_impossible, tr_trips_completed, tr_still_running, tr_teleports,
 * tr_avg_trip_s, tr_avg_waiting_s, tr_avg_time_loss_s, tr_avg_route_m, tr_total_time_loss_h, tr_co2_kg,
 * tr_closed_streets, tr_hotspots (JSON: street, waiting_s, time_loss_s, vehicles), tr_wall_clock_s, tr_status,
 * tr_source; drops tr_closed_edges.
 */
class TrafficSimulateStage extends WPartitionStage {

  override def name: String = "trafficSimulate"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val maxWaitS = args.int(0, 3600)
    rows.map { row =>
      def s(k: String) = String.valueOf(row.fields.getOrElse(k, ""))
      val keep = row.fields - "tr_closed_edges"
      val closed = s("tr_closed_edges").split(" ").toList.filter(x => x.nonEmpty && x != "null")
      val body = s"""{"network":"${TrafficSim.esc(s("tr_network"))}","label":"${TrafficSim.esc(s("tr_scenario"))}",""" +
        s""""closed_edges":[${closed.map(c => "\"" + TrafficSim.esc(c) + "\"").mkString(",")}],""" +
        s""""vehicles_per_hour":${row.double("tr_vph").getOrElse(2000.0).toInt},""" +
        s""""duration_s":${row.double("tr_duration_s").getOrElse(1800.0).toInt},"seed":${row.double("tr_seed").getOrElse(42.0).toInt}}"""
      val cols: Map[String, Any] = Try(TrafficSimulateStage.kpis(TrafficSim.post(s"${s("tr_service").stripSuffix("/")}/api/run", body, maxWaitS)))
        .recover { case e: Throwable =>
          ctx.warn(s"trafficSimulate ${s("tr_scenario")}: ${e.getMessage}")
          TrafficSimulateStage.COLUMNS.map(_ -> "").toMap ++ Map("tr_status" -> ("error:" + String.valueOf(e.getMessage).take(150)))
        }.get
      WRow(keep ++ cols)
    }
  }
}

object TrafficSimulateStage {
  import SatelliteIndicesStage.Json
  import TrafficSim.{esc, str}

  val COLUMNS = Seq("tr_run_id", "tr_trips_planned", "tr_trips_impossible", "tr_trips_completed", "tr_still_running",
    "tr_teleports", "tr_avg_trip_s", "tr_avg_waiting_s", "tr_avg_time_loss_s", "tr_avg_route_m", "tr_total_time_loss_h",
    "tr_co2_kg", "tr_closed_streets", "tr_hotspots", "tr_wall_clock_s", "tr_status", "tr_source")

  def kpis(o: Map[String, Any]): Map[String, Any] = {
    def n(k: String) = Json.num(o.get(k))
    def i(k: String) = { val v = n(k); if (v.isNaN) 0 else v.toInt }
    val hot = Json.arr(o.get("hotspots")).map { h0 =>
      val h = Json.obj(h0)
      s"""{"street":"${esc(str(h, "street"))}","waiting_s":${i0(h, "waiting_s")},"time_loss_s":${i0(h, "time_loss_s")},"vehicles":${i0(h, "vehicles")}}"""
    }
    Map[String, Any](
      "tr_run_id" -> str(o, "run_id"), "tr_trips_planned" -> i("trips_planned"), "tr_trips_impossible" -> i("trips_impossible"),
      "tr_trips_completed" -> i("trips_completed"), "tr_still_running" -> i("still_running_at_end"), "tr_teleports" -> i("teleports"),
      "tr_avg_trip_s" -> n("avg_trip_duration_s"), "tr_avg_waiting_s" -> n("avg_waiting_s"), "tr_avg_time_loss_s" -> n("avg_time_loss_s"),
      "tr_avg_route_m" -> n("avg_route_length_m"), "tr_total_time_loss_h" -> n("total_time_loss_h"), "tr_co2_kg" -> n("total_co2_kg"),
      "tr_closed_streets" -> Json.arr(o.get("closed_streets")).map(String.valueOf).mkString(", "),
      "tr_hotspots" -> hot.mkString("[", ",", "]"), "tr_wall_clock_s" -> n("wall_clock_s"),
      "tr_status" -> "ok", "tr_source" -> str(o, "source"))
  }

  private def i0(h: Map[String, Any], k: String): Int = { val v = Json.num(h.get(k)); if (v.isNaN) 0 else v.toInt }
}
