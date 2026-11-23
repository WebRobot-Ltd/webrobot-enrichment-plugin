package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WRow, WebroStageContext}

import scala.util.Try

/**
 * Italian flood and landslide hazard (PAI national mosaics, ISPRA IdroGEO) at each row's coordinates — FREE, no API key.
 *
 * Calls the public IdroGEO API (`/api/pir/verifica_pericolosita_at_point`) with a circle of `radius_m` metres around
 * the point and returns every hazard class that touches it. Italy only (outside Italy the columns stay empty with
 * hz_status "outside_italy_or_none"). Licences (ISPRA open data): landslide mosaic CC BY 4.0, flood mosaic
 * CC BY-SA 4.0 — cite "ISPRA, Mosaicature nazionali di pericolosità idraulica e da frana (IdroGEO)"; a published
 * dataset derived from the flood mosaic must keep CC BY-SA. The binding maps are those of the River Basin
 * District Authorities: this is a screening layer, not a certificate.
 *
 * Pipeline YAML:
 * {{{
 * - stage: floodLandslideHazard
 *   args:
 *     - "lat"   # latitude column (WGS84)
 *     - "lon"   # longitude column (WGS84)
 *     - 100     # radius around the point, metres (30-500, default 100)
 * }}}
 * Adds hz_flood_classes (e.g. "P1,P2"), hz_flood_max (0-3: 3 = P3 high, frequent floods), hz_landslide_classes
 * (e.g. "P2,P4", "AA" = attention area), hz_landslide_max (0-4), hz_municipality, hz_basin_authority, hz_status,
 * hz_source.
 */
class FloodLandslideHazardStage extends RowEnricher {

  override def name: String = "floodLandslideHazard"

  override protected def cacheKey(row: WRow, args: WArgs): String = {
    val latF = args.string(0, "lat")
    val lonF = args.string(1, "lon")
    (row.double(latF), row.double(lonF)) match {
      case (Some(a), Some(b)) if !a.isNaN && !b.isNaN => f"$a%.5f,$b%.5f"
      case _ => ""
    }
  }

  override protected def enrich(row: WRow, args: WArgs, ctx: WebroStageContext): Map[String, Any] = {
    val latF = args.string(0, "lat")
    val lonF = args.string(1, "lon")
    val radiusMetres = args.int(2, 100)
    val r = math.max(30, math.min(500, radiusMetres))
    Try(FloodLandslideHazardStage.lookup(row.double(latF).get, row.double(lonF).get, r, ctx)).recover { case e: Throwable =>
      // every column always present, even when the service fails: a missing column breaks the SQL downstream
      FloodLandslideHazardStage.COLUMNS.map(c => c -> "").toMap ++ Map("hz_status" -> ("error:" + String.valueOf(e.getMessage).take(100)),
        "hz_source" -> FloodLandslideHazardStage.SOURCE)
    }.get
  }
}

object FloodLandslideHazardStage {
  val API    = "https://idrogeo.isprambiente.it/api/pir/verifica_pericolosita_at_point"
  val SOURCE = "ISPRA IdroGEO, mosaicature nazionali PAI (frane CC BY 4.0, alluvioni CC BY-SA 4.0)"
  import SatelliteIndicesStage.Json
  val COLUMNS = Seq("hz_flood_classes", "hz_flood_max", "hz_landslide_classes", "hz_landslide_max", "hz_municipality",
    "hz_basin_authority", "hz_status", "hz_source")

  def lookup(lat: Double, lon: Double, radius: Int, ctx: WebroStageContext): Map[String, Any] = {
    val wkt = java.net.URLEncoder.encode(s"POINT($lon $lat)", "UTF-8").replace("+", "%20")
    val txt = ctx.httpGet(s"$API?center_wkt=$wkt&radius_meter=$radius", Map("Accept" -> "application/json"), 30000)
    val o = Json.obj(Json.parse(txt))
    def classes(k: String): Seq[Int] = Json.arr(o.get(k)).flatMap(v => Try(String.valueOf(v).toDouble.toInt).toOption).distinct.sorted
    val flood = classes("pericolosita_idraulica")
    val slide = classes("pericolosita_frane")
    val com   = o.get("nome_com").map(String.valueOf).filter(s => s != "null").getOrElse("")
    Map(
      "hz_flood_classes"     -> flood.map(c => s"P$c").mkString(","),
      "hz_flood_max"         -> (if (flood.isEmpty) 0 else flood.max),
      "hz_landslide_classes" -> slide.map(c => if (c == 0) "AA" else s"P$c").mkString(","),
      "hz_landslide_max"     -> (if (slide.isEmpty) 0 else slide.max),
      "hz_municipality"      -> com,
      "hz_basin_authority"   -> o.get("nome_adbd").map(String.valueOf).filter(s => s != "null").getOrElse(""),
      "hz_status"            -> (if (com.isEmpty) "outside_italy_or_none" else "ok"),
      "hz_source"            -> SOURCE)
  }
}
