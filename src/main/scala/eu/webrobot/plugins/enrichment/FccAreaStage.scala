package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * FCC Census Area enrichment — FREE, no API key. Maps a US lat/lon to its census geography: county FIPS,
 * state and county name (and the 15-digit census block). Pairs with geocode/censusGeocode to attach US
 * administrative codes for joins to Census/BLS data.
 *
 * Pipeline YAML:
 * {{{
 * - stage: fccArea
 *   args:
 *     - "geo_lat"     # latitude column  (default "geo_lat")
 *     - "geo_lon"     # longitude column (default "geo_lon")
 * }}}
 * Adds fcc_county_fips, fcc_state, fcc_county, fcc_block_fips.
 */
class FccAreaStage extends WPartitionStage {

  override def name: String = "fccArea"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val latF = args.string(0, "geo_lat")
    val lonF = args.string(1, "geo_lon")
    val cache = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val lat = row.str(latF).getOrElse("").trim
      val lon = row.str(lonF).getOrElse("").trim
      if (lat.isEmpty || lon.isEmpty) row
      else {
        val d = cache.getOrElseUpdate(s"$lat,$lon", Try(FccAreaStage.fetch(lat, lon, ctx)).getOrElse(Map.empty))
        d.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
      }
    }
  }
}

object FccAreaStage {
  private val COUNTY_FIPS = """"county_fips"\s*:\s*"(\d+)"""".r
  private val STATE       = """"state_code"\s*:\s*"(\w+)"""".r
  private val COUNTY_NAME = """"county_name"\s*:\s*"([^"]+)"""".r
  private val BLOCK       = """"block_fips"\s*:\s*"(\d+)"""".r

  def fetch(lat: String, lon: String, ctx: WebroStageContext): Map[String, Any] = {
    val url = s"https://geo.fcc.gov/api/census/area?lat=${enc(lat)}&lon=${enc(lon)}&format=json"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/json"), 30000)).getOrElse("")
    if (body.isEmpty || !body.contains("county_fips")) Map.empty
    else Seq(
      COUNTY_FIPS.findFirstMatchIn(body).map("fcc_county_fips" -> _.group(1)),
      STATE.findFirstMatchIn(body).map("fcc_state" -> _.group(1)),
      COUNTY_NAME.findFirstMatchIn(body).map("fcc_county" -> _.group(1)),
      BLOCK.findFirstMatchIn(body).map("fcc_block_fips" -> _.group(1))
    ).flatten.toMap
  }
  private def enc(s: String) = java.net.URLEncoder.encode(s, "UTF-8")
}
