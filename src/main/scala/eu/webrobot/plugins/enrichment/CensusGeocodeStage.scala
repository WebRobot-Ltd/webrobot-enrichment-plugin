package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * US Census geocoder enrichment — FREE, no API key. Resolves a US street address to coordinates + the
 * official matched address. Pairs with the fccArea stage (coords → FIPS) for US-territory joins.
 *
 * Pipeline YAML:
 * {{{
 * - stage: censusGeocode
 *   args:
 *     - "address"     # US address column (default "address")
 * }}}
 * Adds cg_lat, cg_lon, cg_matched.
 */
class CensusGeocodeStage extends WPartitionStage {

  override def name: String = "censusGeocode"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field = args.string(0, "address")
    val cache = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val addr = row.str(field).getOrElse("").trim
      if (addr.isEmpty) row
      else {
        val g = cache.getOrElseUpdate(addr, Try(CensusGeocodeStage.fetch(addr, ctx)).getOrElse(Map.empty))
        g.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
      }
    }
  }
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
