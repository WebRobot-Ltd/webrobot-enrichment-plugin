package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WebroStageContext}

import scala.util.Try

/**
 * GLEIF (Global Legal Entity Identifier) enrichment — FREE, no API key. Resolves a company legal name to
 * its LEI + registration status + legal jurisdiction. B2B / fintech / KYC enrichment.
 *
 * Pipeline YAML:
 * {{{
 * - stage: gleif
 *   args: [{ on: company }]   # legal-name column (default "company")
 * }}}
 * Adds lei, lei_status, lei_jurisdiction.
 */
class GleifStage extends KeyedEnricher {

  override def name: String = "gleif"

  override protected def defaultKey: String = "company"

  override protected def enrich(key: String, args: WArgs, ctx: WebroStageContext): Map[String, Any] =
    GleifStage.fetch(key, ctx)
}

object GleifStage {
  private val LEI    = """"id"\s*:\s*"([A-Z0-9]{20})"""".r
  private val STATUS = """"status"\s*:\s*"([A-Z]+)"""".r
  private val JURIS  = """"jurisdiction"\s*:\s*"([A-Z]{2}[A-Z-]*)"""".r

  def fetch(nameOrLei: String, ctx: WebroStageContext): Map[String, Any] = {
    val isLei = nameOrLei.matches("[A-Z0-9]{20}")
    val url =
      if (isLei) s"https://api.gleif.org/api/v1/lei-records/$nameOrLei"
      else s"https://api.gleif.org/api/v1/lei-records?filter[entity.legalName]=${java.net.URLEncoder.encode(nameOrLei, "UTF-8")}&page[size]=1"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/vnd.api+json"), 30000)).getOrElse("")
    if (body.isEmpty || !body.contains("\"id\"")) Map.empty
    else Seq(
      LEI.findFirstMatchIn(body).map("lei" -> _.group(1)),
      STATUS.findFirstMatchIn(body).map("lei_status" -> _.group(1)),
      JURIS.findFirstMatchIn(body).map("lei_jurisdiction" -> _.group(1))
    ).flatten.toMap
  }
}
