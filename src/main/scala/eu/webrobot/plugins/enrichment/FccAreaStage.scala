package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WRow, WebroStageContext}

import scala.util.Try

/**
 * FCC Census Area enrichment — FREE, no API key. Maps a US lat/lon to its census geography: county FIPS,
 * state and county name (and the 15-digit census block). A SPATIAL join on a composite (lat, lon) key —
 * modeled on [[RowEnricher]] (the criterion is point-contains, resolved server-side by the FCC API).
 *
 * Pipeline YAML:
 * {{{
 * - stage: fccArea
 *   args: [{ lat: geo_lat, lon: geo_lon }]   # or positional ["geo_lat", "geo_lon"]
 * }}}
 * Adds fcc_county_fips, fcc_state, fcc_county, fcc_block_fips.
 */
class FccAreaStage extends RowEnricher {

  override def name: String = "fccArea"

  override protected def cacheKey(row: WRow, args: WArgs): String = {
    val (latF, lonF) = FccAreaStage.cols(args)
    val lat = row.str(latF).getOrElse("").trim
    val lon = row.str(lonF).getOrElse("").trim
    if (lat.isEmpty || lon.isEmpty) "" else s"$lat,$lon"
  }

  override protected def enrich(row: WRow, args: WArgs, ctx: WebroStageContext): Map[String, Any] = {
    val (latF, lonF) = FccAreaStage.cols(args)
    FccAreaStage.fetch(row.str(latF).getOrElse("").trim, row.str(lonF).getOrElse("").trim, ctx)
  }
}

object FccAreaStage {
  private val COUNTY_FIPS = """"county_fips"\s*:\s*"(\d+)"""".r
  private val STATE       = """"state_code"\s*:\s*"(\w+)"""".r
  private val COUNTY_NAME = """"county_name"\s*:\s*"([^"]+)"""".r
  private val BLOCK       = """"block_fips"\s*:\s*"(\d+)"""".r

  /** (latCol, lonCol) from the YAML map form {lat,lon} or positional args(0),(1). */
  def cols(args: WArgs): (String, String) = {
    val s = JoinSpec.from(args, "")
    (s.extra("lat", args.string(0, "geo_lat")), s.extra("lon", args.string(1, "geo_lon")))
  }

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
