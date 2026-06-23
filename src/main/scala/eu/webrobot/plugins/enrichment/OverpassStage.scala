package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * OpenStreetMap Overpass enrichment — FREE, no API key (rate-limited). For each row with a lat/lon, counts
 * nearby OSM features of a given key=value within a radius. The "what's around here" primitive for
 * location intelligence (e.g. count hospitals/schools/restaurants near a property).
 *
 * Pipeline YAML:
 * {{{
 * - stage: overpass
 *   args:
 *     - "geo_lat"        # latitude column (default "geo_lat" — pairs with the geocode stage)
 *     - "geo_lon"        # longitude column (default "geo_lon")
 *     - "amenity=hospital"  # OSM tag selector key=value (default "amenity=hospital")
 *     - 1000             # radius in metres (default 1000)
 *     - "osm_count"      # output column (default "osm_count")
 * }}}
 * Parses the Overpass `out count` JSON with regex to stay dependency-free (SDK-only jar).
 */
class OverpassStage extends WPartitionStage {

  override def name: String = "overpass"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val latF   = args.string(0, "geo_lat")
    val lonF   = args.string(1, "geo_lon")
    val tag    = args.string(2, "amenity=hospital")
    val radius = args.int(3, 1000)
    val outCol = args.string(4, "osm_count")
    val base   = args.string(5, "https://overpass-api.de/api/interpreter")
    val cache  = mutable.Map.empty[String, Option[String]]
    rows.map { row =>
      val lat = row.str(latF).getOrElse("").trim
      val lon = row.str(lonF).getOrElse("").trim
      if (lat.isEmpty || lon.isEmpty) row
      else {
        val cnt = cache.getOrElseUpdate(s"$lat,$lon|$tag|$radius",
          Try(OverpassStage.count(base, lat, lon, tag, radius, ctx)).toOption.flatten)
        cnt.map(c => row.set(outCol, c)).getOrElse(row)
      }
    }
  }
}

object OverpassStage {
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
