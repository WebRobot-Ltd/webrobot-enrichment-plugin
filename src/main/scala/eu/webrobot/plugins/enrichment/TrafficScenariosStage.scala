package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.util.Try

/**
 * Turns the road edges of a network (from trafficNetwork, enriched with a hazard class) into impact SCENARIOS — one
 * output row per scenario, carrying the edges it closes.
 *
 * Always emits a "baseline" (nothing closed). For each hazard level L in `levels` it emits "hazard_ge_L", closing every
 * edge whose hazard column is >= L (with floodLandslideHazard: 3 = P3 frequent floods only, 1 = P1 rare floods, the
 * widest area). `manual` adds what-if closures by street name: "name:street a|street b;name2:street c" (partial,
 * case-insensitive match). Put all the edges of a network in one partition first (`repartition` 1); then `repartition`
 * to the number of scenarios so that each scenario becomes one Spark task (stage trafficSimulate).
 *
 * Pipeline YAML:
 * {{{
 * - stage: trafficScenarios
 *   args:
 *     - "hz_flood_max"   # hazard column (numeric class; "" = no hazard scenarios)
 *     - "3,2,1"          # hazard levels, one scenario each
 *     - 2000             # vehicles per hour (random origin-destination demand)
 *     - 1800             # demand duration, seconds
 *     - 42               # demand seed (same seed = same trips in every scenario)
 *     - ""               # manual closures "name:street|street;name2:street"
 * }}}
 * Output: tr_service, tr_network, tr_scenario, tr_closed_edges (space separated), tr_closed_count, tr_closed_km,
 * tr_vph, tr_duration_s, tr_seed.
 */
class TrafficScenariosStage extends WPartitionStage {

  override def name: String = "trafficScenarios"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val hazardF  = args.string(0, "hz_flood_max")
    val levelsS  = args.string(1, "3,2,1")
    val vph      = args.int(2, 2000)
    val duration = args.int(3, 1800)
    val seed     = args.int(4, 42)
    val manualS  = args.string(5, "")
    val levels = levelsS.split(",").toList.flatMap(s => Try(s.trim.toDouble).toOption)
    val manual = manualS.split(";").toList.filter(_.contains(":")).map { s =>
      val Array(n, sts) = s.split(":", 2); (n.trim, sts.split("\\|").toList.map(_.trim.toLowerCase).filter(_.nonEmpty))
    }
    case class E(service: String, id: String, street: String, len: Double, hz: Double)
    val edges = rows.toList.flatMap { r =>
      val id = String.valueOf(r.fields.getOrElse("tr_edge_id", ""))
      if (id.isEmpty || id == "null") None
      else Some(E(String.valueOf(r.fields.getOrElse("tr_service", TrafficSim.DEFAULT_URL)),
        id, String.valueOf(r.fields.getOrElse("tr_street", "")), r.double("tr_length_m").getOrElse(0.0),
        if (hazardF.isEmpty) 0.0 else r.double(hazardF).filter(!_.isNaN).getOrElse(0.0))) .map(e => (String.valueOf(r.fields.getOrElse("tr_network", "")), e))
    }
    edges.groupBy(_._1).toList.flatMap { case (network, es0) =>
      val es = es0.map(_._2)
      val service = es.head.service
      def scenario(name: String, closed: Seq[E]): WRow = WRow(Map[String, Any](
        "tr_service" -> service, "tr_network" -> network, "tr_scenario" -> name,
        "tr_closed_edges" -> closed.map(_.id).mkString(" "), "tr_closed_count" -> closed.size,
        "tr_closed_km" -> math.round(closed.map(_.len).sum / 10.0) / 100.0,
        "tr_vph" -> vph, "tr_duration_s" -> duration, "tr_seed" -> seed))
      val hazard = levels.map { l =>
        scenario(s"hazard_ge_${if (l == l.floor) l.toInt.toString else l.toString}", es.filter(_.hz >= l))
      }
      val what = manual.map { case (n, sts) => scenario(n, es.filter(e => sts.exists(s => e.street.toLowerCase.contains(s)))) }
      (scenario("baseline", Nil) +: hazard) ++ what
    }.iterator
  }
}
