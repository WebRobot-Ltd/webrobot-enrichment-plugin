package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * OpenStreetMap Nominatim geocoding enrichment — FREE, no API key. Turns a free-text address into
 * coordinates + administrative areas (city/county/state/postcode/country). The georeferencing primitive:
 * feeds downstream territorial joins (e.g. resolve a listing's address → comune/NUTS code → ISTAT/Eurostat).
 *
 * ⚠️ Nominatim usage policy: max 1 request/second and a valid User-Agent. This stage sleeps ~1.1s between
 * DISTINCT addresses (cache dedups repeats per partition) and sends a descriptive User-Agent. For bulk
 * geocoding use a self-hosted Nominatim and override the base URL via arg.
 *
 * Pipeline YAML:
 * {{{
 * - stage: geocode
 *   args:
 *     - "address"                                  # address column (default "address")
 *     - "geo_"                                     # output column prefix (default "geo_")
 *     - "https://nominatim.openstreetmap.org"      # base URL (default public; override for self-hosted)
 * }}}
 * Adds geo_lat, geo_lon, geo_display_name, geo_city, geo_county, geo_state, geo_postcode, geo_country.
 */
class NominatimStage extends WPartitionStage {

  override def name: String = "geocode"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field  = args.string(0, "address")
    val prefix = args.string(1, "geo_")
    val base   = args.string(2, "https://nominatim.openstreetmap.org")
    val cache  = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val addr = row.str(field).getOrElse("").trim
      if (addr.isEmpty) row
      else {
        val g = cache.getOrElseUpdate(addr, Try(NominatimStage.geocode(addr, prefix, base, ctx)).getOrElse(Map.empty))
        g.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
      }
    }
  }
}

object NominatimStage {
  private val LAT = """"lat"\s*:\s*"([^"]+)"""".r
  private val LON = """"lon"\s*:\s*"([^"]+)"""".r
  private val DN  = """"display_name"\s*:\s*"([^"]+)"""".r
  private def part(body: String, key: String): Option[String] =
    new scala.util.matching.Regex("\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*\"([^\"]+)\"")
      .findFirstMatchIn(body).map(_.group(1))

  def geocode(addr: String, prefix: String, base: String, ctx: WebroStageContext): Map[String, Any] = {
    val q = java.net.URLEncoder.encode(addr, "UTF-8")
    val url = s"$base/search?q=$q&format=jsonv2&addressdetails=1&limit=1"
    val body = Try(ctx.httpGet(url,
      Map("User-Agent" -> "WebRobot-Enrichment/1.0 (https://webrobot.eu)", "Accept" -> "application/json"),
      30000)).getOrElse("")
    Try(Thread.sleep(1100)) // honour the 1 req/s public-Nominatim policy
    if (body.isEmpty || body.trim == "[]") Map.empty
    else Seq(
      LAT.findFirstMatchIn(body).map(prefix + "lat" -> _.group(1)),
      LON.findFirstMatchIn(body).map(prefix + "lon" -> _.group(1)),
      DN.findFirstMatchIn(body).map(prefix + "display_name" -> _.group(1)),
      part(body, "city").orElse(part(body, "town")).orElse(part(body, "village")).map(prefix + "city" -> _),
      part(body, "county").map(prefix + "county" -> _),
      part(body, "state").map(prefix + "state" -> _),
      part(body, "postcode").map(prefix + "postcode" -> _),
      part(body, "country").map(prefix + "country" -> _)
    ).flatten.toMap
  }
}
