package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WebroStageContext}

import scala.util.Try

/**
 * US Census geocoder enrichment — FREE, no API key. Resolves a US street address to coordinates + the
 * official matched address. Pairs with the fccArea stage (coords → FIPS) for US-territory joins.
 *
 * Pipeline YAML:
 * {{{
 * - stage: censusGeocode
 *   args: [{ on: address }]   # US address column (default "address")
 * }}}
 * Adds cg_lat, cg_lon, cg_matched.
 */
class CensusGeocodeStage extends KeyedEnricher {

  override def name: String = "censusGeocode"

  override protected def defaultKey: String = "address"

  override protected def enrich(key: String, args: WArgs, ctx: WebroStageContext): Map[String, Any] =
    CensusGeocodeStage.fetch(key, ctx)
}

object CensusGeocodeStage {
  // "coordinates":{"x":-77.035,"y":38.898}   (x=lon, y=lat)
  private val COORD   = """"coordinates"\s*:\s*\{\s*"x"\s*:\s*(-?[\d.]+)\s*,\s*"y"\s*:\s*(-?[\d.]+)""".r
  private val MATCHED = """"matchedAddress"\s*:\s*"([^"]+)"""".r

  def fetch(addr: String, ctx: WebroStageContext): Map[String, Any] = {
    val q = java.net.URLEncoder.encode(addr, "UTF-8")
    val url = s"https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=$q&benchmark=Public_AR_Current&format=json"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/json"), 30000)).getOrElse("")
    if (body.isEmpty || !body.contains("matchedAddress")) Map.empty
    else {
      val coord = COORD.findFirstMatchIn(body)
      Seq(
        coord.map("cg_lon" -> _.group(1)),
        coord.map("cg_lat" -> _.group(2)),
        MATCHED.findFirstMatchIn(body).map("cg_matched" -> _.group(1))
      ).flatten.toMap
    }
  }
}
