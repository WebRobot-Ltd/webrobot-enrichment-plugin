package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WRow, WebroStageContext}

import java.time.LocalDate
import scala.util.Try

/**
 * Mapped wildfires (EFFIS burnt areas, JRC / Copernicus EMS) near each row's coordinates over the last years — FREE, no API key.
 *
 * Queries the public EFFIS WFS for the burnt-area perimeters (MODIS/Sentinel-2 mapping, fires of roughly 30 ha and
 * more, smaller ones where mapped) intersecting a square of `radius_m` metres around the point, one layer per
 * year plus the current season. Europe, Middle East and North Africa. Licence CC BY 4.0 — cite "EFFIS, European
 * Commission JRC". This is the CONFIRMATION layer for a fire: a spectral burn signal (satelliteIndices nbr) with
 * no EFFIS perimeter is more likely a harvest, ploughing or a small unmapped fire.
 *
 * Pipeline YAML:
 * {{{
 * - stage: burntAreas
 *   args:
 *     - "lat"   # latitude column (WGS84)
 *     - "lon"   # longitude column (WGS84)
 *     - 1000    # half side of the search square, metres (default 1000, max 10000)
 *     - 5       # years back, plus the current season (default 5, max 10)
 * }}}
 * Adds fire_count, fire_inside (true if the point lies inside a mapped perimeter), fire_last_date, fire_last_ha,
 * fire_max_ha, fire_total_ha, fire_communes, fire_status, fire_source.
 */
class BurntAreasStage extends RowEnricher {

  override def name: String = "burntAreas"

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
    val radiusMetres = args.double(2, 1000.0)
    val yearsBack = args.int(3, 5)
    Try(BurntAreasStage.lookup(row.double(latF).get, row.double(lonF).get, math.max(50.0, math.min(10000.0, radiusMetres)),
      math.max(0, math.min(10, yearsBack)), ctx)).recover { case e: Throwable =>
      BurntAreasStage.COLUMNS.map(c => c -> "").toMap ++ Map("fire_status" -> ("error:" + String.valueOf(e.getMessage).take(100)),
        "fire_source" -> BurntAreasStage.SOURCE)
    }.get
  }
}

object BurntAreasStage {
  val WFS    = "https://maps.effis.emergency.copernicus.eu/effis"
  val SOURCE = "EFFIS burnt areas, European Commission JRC / Copernicus EMS (CC BY 4.0)"
  import SatelliteIndicesStage.Json
  val COLUMNS = Seq("fire_count", "fire_inside", "fire_last_date", "fire_last_ha", "fire_max_ha", "fire_total_ha", "fire_communes",
    "fire_status", "fire_source")

  final case class Fire(date: String, ha: Double, commune: String, inside: Boolean)

  def lookup(lat: Double, lon: Double, radius: Double, years: Int, ctx: WebroStageContext): Map[String, Any] = {
    val dLat = radius / 111320.0
    val dLon = radius / (111320.0 * math.max(0.1, math.cos(math.toRadians(lat))))
    // Locale.ROOT: an executor with an Italian default locale would print decimal commas
    val bbox = String.format(java.util.Locale.ROOT, "%.6f,%.6f,%.6f,%.6f,urn:ogc:def:crs:EPSG::4326",
      Double.box(lat - dLat), Double.box(lon - dLon), Double.box(lat + dLat), Double.box(lon + dLon))
    val thisYear = LocalDate.now().getYear
    // the per-year layers stop at the last complete year; the current year is in ".season"
    val layers = "ms:modis.ba.poly.season" +: (1 to years).map(k => s"ms:modis.ba.poly.${thisYear - k}")
    var failed = 0
    val fires = layers.flatMap { layer =>
      val url = s"$WFS?service=WFS&version=2.0.0&request=GetFeature&typeNames=$layer&outputFormat=geojson&count=200&bbox=" +
        java.net.URLEncoder.encode(bbox, "UTF-8")
      Try(ctx.httpGet(url, Map("Accept" -> "application/json"), 60000)).toOption.filter(_.trim.startsWith("{")) match {
        case None => failed += 1; Seq.empty[Fire]
        case Some(txt) =>
          Json.arr(Json.obj(Json.parse(txt)).get("features")).map { f0 =>
            val f = Json.obj(f0); val p = Json.obj(f.get("properties")); val g = Json.obj(f.get("geometry"))
            Fire(String.valueOf(p.getOrElse("FIREDATE", "")).take(10), Json.num(p.getOrElse("AREA_HA", "0")),
              String.valueOf(p.getOrElse("COMMUNE", "")), contains(g, lon, lat))
          }
      }
    }.groupBy(f => (f.date, f.commune, f.ha)).values.map(_.head).toSeq.sortBy(_.date)(Ordering[String].reverse)
    val ha = fires.map(_.ha).filterNot(_.isNaN)
    Map(
      "fire_count"     -> fires.size,
      "fire_inside"    -> fires.exists(_.inside),
      "fire_last_date" -> fires.headOption.map(_.date).getOrElse(""),
      "fire_last_ha"   -> fires.headOption.map(_.ha).filterNot(_.isNaN).getOrElse(""),
      "fire_max_ha"    -> (if (ha.isEmpty) "" else ha.max),
      "fire_total_ha"  -> (if (ha.isEmpty) "" else ha.sum),
      "fire_communes"  -> fires.map(_.commune).filter(_.nonEmpty).distinct.mkString(", "),
      "fire_status"    -> (if (failed == layers.size) "error:effis_unreachable" else if (failed > 0) s"partial:$failed layers failed" else "ok"),
      "fire_source"    -> SOURCE)
  }

  /** point-in-polygon (ray casting) for GeoJSON Polygon / MultiPolygon in lon,lat; holes respected. */
  def contains(geom: Map[String, Any], x: Double, y: Double): Boolean = {
    def ring(r: Seq[Any]): Seq[(Double, Double)] = r.map { p => val a = Json.arr(p); (Json.num(a.head), Json.num(a(1))) }
    def inRing(pts: Seq[(Double, Double)]): Boolean = {
      var inside = false; var j = pts.size - 1
      for (i <- pts.indices) {
        val (xi, yi) = pts(i); val (xj, yj) = pts(j)
        if (((yi > y) != (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi)) inside = !inside
        j = i
      }
      inside
    }
    def inPoly(rings: Seq[Any]): Boolean = {
      val rs = rings.map(r => ring(Json.arr(r)))
      rs.nonEmpty && inRing(rs.head) && !rs.tail.exists(inRing)
    }
    String.valueOf(geom.getOrElse("type", "")) match {
      case "Polygon"      => inPoly(Json.arr(geom.get("coordinates")))
      case "MultiPolygon" => Json.arr(geom.get("coordinates")).exists(p => inPoly(Json.arr(p)))
      case _ => false
    }
  }
}
