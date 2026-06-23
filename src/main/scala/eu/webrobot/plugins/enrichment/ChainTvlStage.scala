package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * DefiLlama chain-TVL enrichment — FREE, no API key. Adds the total DeFi TVL (USD) of a blockchain to each
 * row, keyed by chain name (e.g. "Ethereum", "Arbitrum"). Point-in-time aware: with a date column it
 * returns the chain TVL AS OF that day (no look-ahead) from the chain's historical series; else the latest.
 * A macro DeFi risk-regime signal (capital flowing into / out of a chain).
 *
 * Pipeline YAML:
 * {{{
 * - stage: chainTvl
 *   args:
 *     - "chain"       # chain-name column (default "chain")
 *     - "date"        # date column YYYY-MM-DD for point-in-time (default "date"; missing → latest)
 * }}}
 * Adds chain_tvl_usd (and chain_tvl_date).
 */
class ChainTvlStage extends WPartitionStage {

  override def name: String = "chainTvl"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val field = args.string(0, "chain")
    val dateF = args.string(1, "date")
    val hist = mutable.Map.empty[String, Vector[(Long, Double)]]
    rows.map { row =>
      val chain = row.str(field).getOrElse("").trim
      val date  = row.str(dateF).getOrElse("").trim
      if (chain.isEmpty) row
      else {
        val series = hist.getOrElseUpdate(chain, Try(ChainTvlStage.history(chain, ctx)).getOrElse(Vector.empty))
        val cutoff = if (date.nonEmpty)
          Try(java.time.LocalDate.parse(date).atStartOfDay(java.time.ZoneOffset.UTC).toEpochSecond).getOrElse(Long.MaxValue)
        else Long.MaxValue
        series.filter(_._1 <= cutoff).sortBy(_._1).lastOption match {
          case Some((ts, tvl)) =>
            row.set("chain_tvl_usd", f"$tvl%.2f")
               .set("chain_tvl_date", java.time.Instant.ofEpochSecond(ts).atZone(java.time.ZoneOffset.UTC).toLocalDate.toString)
          case None => row
        }
      }
    }
  }
}

object ChainTvlStage {
  private val POINT = """"date"\s*:\s*(\d+)\s*,\s*"tvl"\s*:\s*([\d.eE+]+)""".r
  def history(chain: String, ctx: WebroStageContext): Vector[(Long, Double)] = {
    val c = java.net.URLEncoder.encode(chain, "UTF-8")
    val body = Try(ctx.httpGet(s"https://api.llama.fi/v2/historicalChainTvl/$c",
      Map("Accept" -> "application/json"), 45000)).getOrElse("")
    POINT.findAllMatchIn(body).map(m => (m.group(1).toLong, m.group(2).toDouble)).toVector
  }
}
