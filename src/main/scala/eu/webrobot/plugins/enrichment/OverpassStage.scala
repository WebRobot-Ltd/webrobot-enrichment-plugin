package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WRow, WebroStageContext}

import scala.util.Try

/**
 * OpenStreetMap Overpass enrichment — FREE, no API key (rate-limited). For each row with a lat/lon, counts
 * nearby OSM features of a given key=value within a radius. A SPATIAL + AGGREGATE join on a composite
 * (lat, lon, tag, radius) key — modeled on [[RowEnricher]] (the criterion is within-radius count).
 *
 * Pipeline YAML:
 * {{{
 * - stage: overpass
 *   args: [{ lat: geo_lat, lon: geo_lon, tag: "amenity=hospital", radius: "1000", out: osm_count }]
 *   # or positional ["geo_lat", "geo_lon", "amenity=hospital", 1000, "osm_count"]
 * }}}
 * Parses the Overpass `out count` JSON with regex to stay dependency-free (SDK-only jar).
 */
class OverpassStage extends RowEnricher {

  override def name: String = "overpass"

  override protected def cacheKey(row: WRow, args: WArgs): String = {
    val p = OverpassStage.params(args)
    val lat = row.str(p.latF).getOrElse("").trim
    val lon = row.str(p.lonF).getOrElse("").trim
    if (lat.isEmpty || lon.isEmpty) "" else s"$lat,$lon|${p.tag}|${p.radius}"
  }

  override protected def enrich(row: WRow, args: WArgs, ctx: WebroStageContext): Map[String, Any] = {
    val p   = OverpassStage.params(args)
    val lat = row.str(p.latF).getOrElse("").trim
    val lon = row.str(p.lonF).getOrElse("").trim
    OverpassStage.count(p.base, lat, lon, p.tag, p.radius, ctx)
      .map(c => Map[String, Any](p.outCol -> c)).getOrElse(Map.empty)
  }
}

object OverpassStage {
  private final case class Params(latF: String, lonF: String, tag: String, radius: Int, outCol: String, base: String)

  private def params(args: WArgs): Params = {
    val s = JoinSpec.from(args, "")
    Params(
      s.extra("lat", args.string(0, "geo_lat")),
      s.extra("lon", args.string(1, "geo_lon")),
      s.extra("tag", args.string(2, "amenity=hospital")),
      Try(s.extra("radius", args.int(3, 1000).toString).toInt).getOrElse(1000),
      s.extra("out", args.string(4, "osm_count")),
      s.extra("base", args.string(5, "https://overpass-api.de/api/interpreter"))
    )
  }

  // out count → {"elements":[{"type":"count","tags":{"total":"3","nodes":"3",...}}]}
  private val TOTAL = """"total"\s*:\s*"?(\d+)"?""".r
  def count(base: String, lat: String, lon: String, tag: String, radius: Int, ctx: WebroStageContext): Option[String] = {
    val (k, v) = tag.split("=", 2) match { case Array(a, b) => (a, b); case _ => (tag, "") }
    val sel = if (v.nonEmpty) s"""["$k"="$v"]""" else s"""["$k"]"""
    val ql =
      s"[out:json][timeout:25];(node$sel(around:$radius,$lat,$lon);way$sel(around:$radius,$lat,$lon););out count;"
    val url = s"$base?data=${java.net.URLEncoder.encode(ql, "UTF-8")}"
    val body = Try(ctx.httpGet(url,
      Map("Accept" -> "application/json", "User-Agent" -> "WebRobot-Enrichment/1.0 (https://webrobot.eu)"),
      45000)).getOrElse("")
    if (body.isEmpty || !body.contains("count")) None
    else TOTAL.findFirstMatchIn(body).map(_.group(1))
  }
}
