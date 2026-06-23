package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * OECD SDMX enrichment — FREE, no API key. Adds an OECD indicator value (latest observation) to each row
 * keyed by a code (typically an ISO country code). Generic: pass the SDMX dataflow + a key TEMPLATE with
 * `{code}` substituted by the row's key, so any OECD dataflow is reachable without code changes.
 *
 * Pipeline YAML (e.g. short-term interest rates):
 * {{{
 * - stage: oecd
 *   args:
 *     - "country"            # key column (default "country")
 *     - "OECD.SDD.STES,DSD_STES@DF_FINMARK,4.0"  # SDMX dataflow id
 *     - "{code}.M.IRSTCI....."  # SDMX key template; {code} -> key value
 *     - "oecd_value"         # output column (default "oecd_value")
 * }}}
 * Parses SDMX-JSON with regex to stay dependency-free (SDK-only jar).
 */
class OecdStage extends WPartitionStage {

  override def name: String = "oecd"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field  = args.string(0, "country")
    val flow   = args.string(1, "")
    val keyTpl = args.string(2, "{code}")
    val outCol = args.string(3, "oecd_value")
    val base   = args.string(4, "https://sdmx.oecd.org/public/rest/data")
    val cache  = mutable.Map.empty[String, Option[String]]
    rows.map { row =>
      val code = row.str(field).getOrElse("").trim
      if (code.isEmpty || flow.isEmpty) row
      else {
        val v = cache.getOrElseUpdate(code,
          Try(OecdStage.fetch(base, flow, keyTpl.replace("{code}", code), ctx)).toOption.flatten)
        v.map(value => row.set(outCol, value)).getOrElse(row)
      }
    }
  }
}

object OecdStage {
  private val OBS = """"observations"\s*:\s*\{\s*"[^"]*"\s*:\s*\[\s*(-?[\d.eE+]+)""".r
  def fetch(base: String, flow: String, key: String, ctx: WebroStageContext): Option[String] = {
    val url = s"$base/$flow/$key?lastNObservations=1&dimensionAtObservation=AllDimensions"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/vnd.sdmx.data+json"), 45000)).getOrElse("")
    if (body.isEmpty || !body.contains("observations")) None
    else OBS.findFirstMatchIn(body).map(_.group(1))
  }
}
