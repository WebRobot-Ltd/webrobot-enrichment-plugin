package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * Banca d'Italia (Infostat SDMX 2.1) enrichment — FREE, no API key. Adds a Banca d'Italia indicator value
 * (latest observation) to each row keyed by a code, via a generic dataflow + key TEMPLATE ({code}).
 *
 * Pipeline YAML:
 * {{{
 * - stage: bancaItalia
 *   args:
 *     - "key"                # key column (default "key")
 *     - "BIP_PUB__BPM6"      # Infostat dataflow id
 *     - "M.{code}...."       # SDMX key template; {code} -> key value
 *     - "bdi_value"          # output column (default "bdi_value")
 * }}}
 * Parses SDMX-JSON with regex to stay dependency-free (SDK-only jar).
 */
class BancaItaliaStage extends WPartitionStage {

  override def name: String = "bancaItalia"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field  = args.string(0, "key")
    val flow   = args.string(1, "")
    val keyTpl = args.string(2, "{code}")
    val outCol = args.string(3, "bdi_value")
    val base   = args.string(4, "https://infostat.bancaditalia.it/inquiry/api/sdmx21/v2")
    val cache  = mutable.Map.empty[String, Option[String]]
    rows.map { row =>
      val code = row.str(field).getOrElse("").trim
      if (code.isEmpty || flow.isEmpty) row
      else {
        val v = cache.getOrElseUpdate(code,
          Try(BancaItaliaStage.fetch(base, flow, keyTpl.replace("{code}", code), ctx)).toOption.flatten)
        v.map(value => row.set(outCol, value)).getOrElse(row)
      }
    }
  }
}

object BancaItaliaStage {
  private val OBS = """"observations"\s*:\s*\{\s*"[^"]*"\s*:\s*\[\s*(-?[\d.eE+]+)""".r
  def fetch(base: String, flow: String, key: String, ctx: WebroStageContext): Option[String] = {
    val url = s"$base/data/$flow/$key?format=jsondata&lastNObservations=1"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/vnd.sdmx.data+json"), 45000)).getOrElse("")
    if (body.isEmpty || !body.contains("observations")) None
    else OBS.findFirstMatchIn(body).map(_.group(1))
  }
}
