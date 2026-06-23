package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * Country metadata enrichment — FREE, no API key (World Bank country endpoint). Adds region, income
 * group, capital and capital coordinates to each row keyed by an ISO country code. A reliable, key-less
 * replacement for the deprecated restcountries.com.
 *
 * Pipeline YAML:
 * {{{
 * - stage: countryInfo
 *   args:
 *     - "country"     # ISO2/ISO3 country-code column (default "country")
 * }}}
 * Adds ci_name, ci_region, ci_income, ci_capital, ci_lat, ci_lon.
 */
class CountryInfoStage extends WPartitionStage {

  override def name: String = "countryInfo"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field = args.string(0, "country")
    val cache = mutable.Map.empty[String, Map[String, Any]]
    rows.map { row =>
      val code = row.str(field).getOrElse("").trim
      if (code.isEmpty) row
      else {
        val data = cache.getOrElseUpdate(code, Try(CountryInfoStage.fetch(code, ctx)).getOrElse(Map.empty))
        data.foldLeft(row) { case (r, (k, v)) => r.set(k, v) }
      }
    }
  }
}

object CountryInfoStage {
  private val NAME   = """"name"\s*:\s*"([^"]+)"""".r
  private val REGION = """"region"\s*:\s*\{[^}]*"value"\s*:\s*"([^"]+)"""".r
  private val INCOME = """"incomeLevel"\s*:\s*\{[^}]*"value"\s*:\s*"([^"]+)"""".r
  private val CAP    = """"capitalCity"\s*:\s*"([^"]*)"""".r
  private val LON    = """"longitude"\s*:\s*"([^"]*)"""".r
  private val LAT    = """"latitude"\s*:\s*"([^"]*)"""".r

  def fetch(code: String, ctx: WebroStageContext): Map[String, Any] = {
    val c = java.net.URLEncoder.encode(code, "UTF-8")
    val body = Try(ctx.httpGet(s"https://api.worldbank.org/v2/country/$c?format=json",
      Map("Accept" -> "application/json"), 30000)).getOrElse("")
    if (body.isEmpty || body.contains("\"message\"")) Map.empty
    else Seq(
      NAME.findFirstMatchIn(body).map("ci_name" -> _.group(1)),
      REGION.findFirstMatchIn(body).map("ci_region" -> _.group(1)),
      INCOME.findFirstMatchIn(body).map("ci_income" -> _.group(1)),
      CAP.findFirstMatchIn(body).map("ci_capital" -> _.group(1)),
      LAT.findFirstMatchIn(body).map("ci_lat" -> _.group(1)),
      LON.findFirstMatchIn(body).map("ci_lon" -> _.group(1))
    ).flatten.toMap
  }
}
