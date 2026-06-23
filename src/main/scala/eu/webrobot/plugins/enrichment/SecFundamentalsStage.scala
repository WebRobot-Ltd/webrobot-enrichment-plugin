package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import scala.collection.mutable
import scala.util.Try

/**
 * SEC EDGAR fundamentals enrichment — FREE, no API key. Joins each row (keyed by US stock ticker) to a
 * company's reported financials (revenue, EPS, net income, …) from the SEC XBRL `companyconcept` API.
 *
 * POINT-IN-TIME via the FILING DATE (the equity look-ahead trap): a quarter's numbers only become public
 * when the 10-K/10-Q is FILED (weeks after period end), so the as-of join keys on `filed`, not on the
 * fiscal-period end — a backtest row dated 2024-02-15 sees Apple's Q1-FY24 only because it was filed
 * 2024-02-02, and would NOT see a quarter filed later. (Equity analog of llamaFees; [[AsOf]] criterion.)
 *
 * Pipeline YAML:
 * {{{
 * - stage: secFundamentals
 *   args: [{ on: ticker, asof: date,
 *            concepts: "RevenueFromContractWithCustomerExcludingAssessedTax,EarningsPerShareDiluted,NetIncomeLoss" }]
 *   # positional ["ticker","date","<concepts csv>"] also works; asof omitted ⇒ latest filed
 * }}}
 * For each concept adds eq_<alias>, eq_<alias>_end (fiscal period end), eq_<alias>_filed (filing date).
 */
class SecFundamentalsStage extends WPartitionStage {

  override def name: String = "secFundamentals"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val spec = JoinSpec.from(args, "ticker", "date")
    val concepts = spec.extra("concepts",
        args.string(1, "RevenueFromContractWithCustomerExcludingAssessedTax,EarningsPerShareDiluted,NetIncomeLoss"))
      .split(",").map(_.trim).filter(_.nonEmpty).toVector
    lazy val cikMap: Map[String, String] = Try(SecFundamentalsStage.tickerCikMap(ctx)).getOrElse(Map.empty)
    // ticker -> (concept -> series of (filedEpoch, (value, periodEnd)))
    val cache = mutable.Map.empty[String, Map[String, Vector[(Long, (String, String))]]]

    rows.map { row =>
      val ticker = row.str(spec.on).getOrElse("").trim.toUpperCase
      val date   = spec.asof.flatMap(c => row.str(c)).getOrElse("").trim
      if (ticker.isEmpty) row
      else {
        val byConcept = cache.getOrElseUpdate(ticker,
          cikMap.get(ticker) match {
            case Some(cik) => concepts.map(c => c -> Try(SecFundamentalsStage.series(cik, c, ctx)).getOrElse(Vector.empty)).toMap
            case None      => Map.empty
          })
        val cols = concepts.flatMap { c =>
          val series = byConcept.getOrElse(c, Vector.empty)
          val picked = if (date.nonEmpty) AsOf.pick(series, date) else series.sortBy(_._1).lastOption
          picked.toSeq.flatMap { case (ts, (v, end)) =>
            val a = SecFundamentalsStage.alias(c)
            Seq(s"eq_$a" -> v, s"eq_${a}_end" -> end, s"eq_${a}_filed" -> AsOf.toDate(ts))
          }
        }.toMap
        Enrichers.augment(row, cols)
      }
    }
  }
}

object SecFundamentalsStage {
  private val UA = Map("User-Agent" -> "WebRobot Enrichment research@webrobot.eu", "Accept" -> "application/json")

  // company_tickers.json: {"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}, ...}
  private val TICK = """"cik_str"\s*:\s*(\d+)\s*,\s*"ticker"\s*:\s*"([^"]+)"""".r
  // companyconcept datapoint (field order is stable in the SEC XBRL JSON):
  // {..."end":"2023-12-30","val":119575000000,"accn":"...","fy":2024,"fp":"Q1","form":"10-Q","filed":"2024-02-02"...}
  private val DP =
    """"end"\s*:\s*"([\d-]+)"\s*,\s*"val"\s*:\s*(-?[\d.eE+]+)\s*,\s*"accn"\s*:\s*"[^"]*"\s*,\s*"fy"\s*:\s*\d+\s*,\s*"fp"\s*:\s*"[^"]*"\s*,\s*"form"\s*:\s*"([^"]+)"\s*,\s*"filed"\s*:\s*"([\d-]+)"""".r

  private val ALIAS = Map(
    "revenuefromcontractwithcustomerexcludingassessedtax" -> "revenue",
    "revenues" -> "revenue",
    "earningspersharediluted" -> "eps",
    "earningspersharebasic" -> "eps_basic",
    "netincomeloss" -> "net_income",
    "assets" -> "assets",
    "liabilities" -> "liabilities",
    "stockholdersequity" -> "equity"
  )
  def alias(c: String): String = ALIAS.getOrElse(c.toLowerCase, c.toLowerCase.replaceAll("[^a-z0-9]+", "_"))

  def tickerCikMap(ctx: WebroStageContext): Map[String, String] = {
    val body = Try(ctx.httpGet("https://www.sec.gov/files/company_tickers.json", UA, 45000)).getOrElse("")
    TICK.findAllMatchIn(body).map(m => m.group(2).toUpperCase -> f"${m.group(1).toLong}%010d").toMap
  }

  /** annual/quarterly reports only (10-K/10-Q + amendments), as (filedEpoch, (value, periodEnd)). */
  def series(cik: String, concept: String, ctx: WebroStageContext): Vector[(Long, (String, String))] = {
    val body = Try(ctx.httpGet(s"https://data.sec.gov/api/xbrl/companyconcept/CIK$cik/us-gaap/$concept.json", UA, 45000)).getOrElse("")
    DP.findAllMatchIn(body).flatMap { m =>
      val (end, v, form, filed) = (m.group(1), m.group(2), m.group(3), m.group(4))
      if (!form.startsWith("10-")) None
      else AsOf.parseDay(filed).map(ts => (ts, (v, end)))
    }.toVector.sortBy(t => (t._1, t._2._2)) // by filed, then period end
  }
}
