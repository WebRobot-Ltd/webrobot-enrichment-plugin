package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * Open-Meteo current-weather enrichment — FREE, no API key. Adds current temperature, wind speed and
 * weather code to each row keyed by latitude/longitude (pairs naturally with the geocode stage).
 *
 * Pipeline YAML:
 * {{{
 * - stage: openMeteo
 *   args:
 *     - "geo_lat"     # latitude column  (default "geo_lat")
 *     - "geo_lon"     # longitude column (default "geo_lon")
 * }}}
 * Adds wx_temp_c, wx_windspeed, wx_weathercode.
 */
class OpenMeteoStage extends WPartitionStage {

  override def name: String = "openMeteo"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val latF = args.string(0, "geo_lat")
    val lonF = args.string(1, "geo_lon")
    val cache = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val lat = row.str(latF).getOrElse("").trim
      val lon = row.str(lonF).getOrElse("").trim
      if (lat.isEmpty || lon.isEmpty) row
      else {
        val w = cache.getOrElseUpdate(s"$lat,$lon", Try(OpenMeteoStage.fetch(lat, lon, ctx)).getOrElse(Map.empty))
        w.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
      }
    }
  }
}

object OpenMeteoStage {
  // "current_weather":{"temperature":18.3,"windspeed":7.1,"winddirection":180,"weathercode":2,...}
  private val TEMP = """"temperature"\s*:\s*(-?[\d.]+)""".r
  private val WIND = """"windspeed"\s*:\s*(-?[\d.]+)""".r
  private val CODE = """"weathercode"\s*:\s*(\d+)""".r

  def fetch(lat: String, lon: String, ctx: WebroStageContext): Map[String, Any] = {
    val url = s"https://api.open-meteo.com/v1/forecast?latitude=${enc(lat)}&longitude=${enc(lon)}&current_weather=true"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/json"), 30000)).getOrElse("")
    if (body.isEmpty || !body.contains("current_weather")) Map.empty
    else Seq(
      TEMP.findFirstMatchIn(body).map("wx_temp_c" -> _.group(1)),
      WIND.findFirstMatchIn(body).map("wx_windspeed" -> _.group(1)),
      CODE.findFirstMatchIn(body).map("wx_weathercode" -> _.group(1))
    ).flatten.toMap
  }
  private def enc(s: String) = java.net.URLEncoder.encode(s, "UTF-8")
}
