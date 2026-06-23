package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * ISTAT (Italian National Statistics) SDMX enrichment — FREE, no API key. Adds an ISTAT indicator value
 * (latest observation) to each row keyed by an ISTAT territory code (comune / provincia / regione).
 * Italian territorial ground-truth for real-estate arbitrage & market intelligence (population, income,
 * prices, ...). Generic by design: you pass the SDMX dataflow id and a key TEMPLATE in which `{code}` is
 * replaced by the row's territory code, so any ISTAT dataflow is reachable without code changes.
 *
 * Context-aware PARTITION stage: each executor fetches+caches every (dataflow,key) it sees ONCE per
 * partition. Adds `<out>` (the observation value) and `<out>_period` (the time period, when present).
 *
 * Pipeline YAML (resident population by municipality, dataflow 22_289, last observation):
 * {{{
 * - stage: istat
 *   args:
 *     - "territory"          # column holding the ISTAT territory code (default "territory")
 *     - "22_289"             # ISTAT SDMX dataflow id
 *     - "A.{code}.JAN.99.9"  # SDMX key template; {code} → territory code (dot-separated dimensions)
 *     - "istat_value"        # output column (default "istat_value")
 * }}}
 *
 * Parses SDMX-JSON (vnd.sdmx.data+json) with regex to stay dependency-free (SDK-only jar). The exact key
 * template/dimension order is dataflow-specific — inspect a dataflow with the ISTAT SDMX `dataflow`/
 * `datastructure` endpoints, then plug it in here.
 */
class IstatStage extends WPartitionStage {

  override def name: String = "istat"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field  = args.string(0, "territory")
    val flow   = args.string(1, "")
    val keyTpl = args.string(2, "{code}")
    val outCol = args.string(3, "istat_value")
    val base   = args.string(4, "https://esploradati.istat.it/SDMXWS/rest")
    val cache  = mutable.Map.empty[String, Option[(String, String)]]
    rows.map { row =>
      val code = row.str(field).getOrElse("").trim
      if (code.isEmpty || flow.isEmpty) row
      else {
        val res = cache.getOrElseUpdate(code,
          Try(IstatStage.fetch(base, flow, keyTpl.replace("{code}", code), ctx)).toOption.flatten)
        res match {
          case Some((value, period)) => row.set(outCol, value).set(s"${outCol}_period", period)
          case None                  => row
        }
      }
    }
  }
}

object IstatStage {
  // SDMX-JSON observations: "observations":{"0:0:..":[<value>, ...]}  (first number = the value)
  private val OBS    = """"observations"\s*:\s*\{\s*"[^"]*"\s*:\s*\[\s*(-?[\d.eE+]+)""".r
  // time-period dimension values list: "values":[{"id":"2023",...}] under the time dimension (best-effort)
  private val PERIOD = """"id"\s*:\s*"((?:19|20)\d{2}(?:-[A-Za-z0-9]+)?)"""".r

  def fetch(base: String, flow: String, key: String, ctx: WebroStageContext): Option[(String, String)] = {
    val url = s"$base/data/$flow/$key/?format=jsondata&lastNObservations=1&detail=dataonly"
    val body = Try(ctx.httpGet(url, Map("Accept" -> "application/vnd.sdmx.data+json"), 45000)).getOrElse("")
    if (body.isEmpty || !body.contains("observations")) None
    else OBS.findFirstMatchIn(body).map { m =>
      val period = PERIOD.findFirstMatchIn(body).map(_.group(1)).getOrElse("")
      (m.group(1), period)
    }
  }
}
