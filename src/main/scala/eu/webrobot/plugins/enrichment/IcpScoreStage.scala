package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.util.Try

/**
 * Scores GTM signals against a CONFIGURABLE ideal-customer profile (ICP) and gates them — one output row per input
 * row, with the score, the gate decision and the reason written out.
 *
 * The profiles are JSON (inline in the args, or a path in the organisation's storage), so each portfolio company keeps
 * its own ICP without code: countries, keywords that must / must not appear, the weight of each signal type, how fast a
 * signal goes stale, value bands and the minimum score to pass. score = 100 × fit × signal weight × freshness, where fit
 * comes from the keywords (and the value band when a value column is set). A row can be forced in or out with an
 * override column ("include" / "exclude"): the gate keeps the decision, the reason says it was overridden.
 *
 * Profile JSON (a list, or {"profiles":[…]}):
 * {{{
 * {"name": "public-sector-it", "countries": ["IT"], "min_score": 45, "freshness_days": 30,
 *  "include_keywords": ["software", "dati", "cloud", "intelligenza artificiale"],
 *  "exclude_keywords": ["pulizia", "manutenzione impianti"],
 *  "signal_weights": {"tender": 1.0, "hiring": 0.8, "funding": 0.7, "inbound": 1.0, "conversation": 0.5},
 *  "value_bands": [[1000000, 1.0], [200000, 0.85], [50000, 0.65], [0, 0.45]],
 *  "roles": ["CIO", "Responsabile sistemi informativi", "RUP"]}
 * }}}
 *
 * Pipeline YAML:
 * {{{
 * - stage: icpScore
 *   args:
 *     - "s3a://…/gtm/icp_profiles.json"   # profiles: inline JSON or a storage path
 *     - "public-sector-it"                 # profile to apply
 *     - "signal_summary,company"           # text columns the keywords are matched in
 *     - "signal_type"                      # signal type column
 *     - "signal_date"                      # signal date column (YYYY-MM-DD…)
 *     - ""                                 # value column, e.g. estimated value ("" = none)
 *     - ""                                 # country column, ISO-2 ("" = no country check)
 *     - ""                                 # override column: include / exclude ("" = none)
 * }}}
 * Adds icp_profile, icp_score (0-100), icp_fit, icp_gate (pass / reject / override-pass / override-reject),
 * icp_reason, icp_roles (comma-separated, for the agent that looks for people).
 */
class IcpScoreStage extends WPartitionStage {

  override def name: String = "icpScore"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val profilesArg = args.string(0, "")
    val profileName = args.string(1, "default")
    val textF       = args.string(2, "signal_summary,company")
    val typeF       = args.string(3, "signal_type")
    val dateF       = args.string(4, "signal_date")
    val valueF      = args.string(5, "")
    val countryF    = args.string(6, "")
    val overrideF   = args.string(7, "")
    val profile = Try(Icp.load(profilesArg, profileName, ctx)).recover { case e: Throwable =>
      ctx.warn(s"icpScore: ${e.getMessage}"); null
    }.get
    if (profile == null)
      return rows.map(r => Enrichers.augment(r, Map("icp_profile" -> profileName, "icp_gate" -> "reject",
        "icp_reason" -> s"error: profile '$profileName' not loaded")))
    val textCols = textF.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    val today = LocalDate.now()
    rows.map { r =>
      def s(f: String) = if (f.isEmpty) "" else Option(r.fields.getOrElse(f, "")).map(String.valueOf).filter(_ != "null").getOrElse("").trim
      Enrichers.augment(r, Icp.score(profile, textCols.map(s).mkString(" "), s(typeF).toLowerCase,
        s(dateF), if (valueF.isEmpty) None else r.double(valueF).filter(v => !v.isNaN), s(countryF).toUpperCase,
        s(overrideF).toLowerCase, today))
    }
  }
}

object Icp {
  import SatelliteIndicesStage.Json

  final case class Profile(name: String, countries: Set[String], include: Seq[String], exclude: Seq[String],
                           weights: Map[String, Double], freshnessDays: Double, minScore: Double,
                           valueBands: Seq[(Double, Double)], roles: Seq[String])

  private def strs(v: Any): Seq[String] = Json.arr(v).map(x => String.valueOf(x).trim).filter(_.nonEmpty)

  def parse(o: Map[String, Any]): Profile = Profile(
    name = String.valueOf(o.getOrElse("name", "default")),
    countries = strs(o.get("countries")).map(_.toUpperCase).toSet,
    include = strs(o.get("include_keywords")).map(_.toLowerCase),
    exclude = strs(o.get("exclude_keywords")).map(_.toLowerCase),
    weights = Json.obj(o.get("signal_weights")).map { case (k, v) => k.toLowerCase -> Json.num(v) }.filter(!_._2.isNaN),
    freshnessDays = Some(Json.num(o.get("freshness_days"))).filter(v => !v.isNaN && v > 0).getOrElse(30.0),
    minScore = Some(Json.num(o.get("min_score"))).filter(v => !v.isNaN).getOrElse(50.0),
    valueBands = Json.arr(o.get("value_bands")).map(Json.arr).filter(_.size >= 2).map(b => (Json.num(b.head), Json.num(b(1))))
      .filter(b => !b._1.isNaN && !b._2.isNaN).sortBy(-_._1),
    roles = strs(o.get("roles")))

  /** `src` is inline JSON, or a storage path (s3a://… or a relative key) read with ctx.storageGet. */
  def load(src: String, name: String, ctx: WebroStageContext): Profile = {
    val txt = if (src.trim.startsWith("{") || src.trim.startsWith("[")) src
              else new String(ctx.storageGet(src.trim), "UTF-8")
    val root = Json.parse(txt)
    val list = root match {
      case m: Map[_, _] if m.asInstanceOf[Map[String, Any]].contains("profiles") => Json.arr(m.asInstanceOf[Map[String, Any]]("profiles"))
      case m: Map[_, _] => Seq(m)
      case other => Json.arr(other)
    }
    list.map(x => parse(Json.obj(x))).find(_.name.equalsIgnoreCase(name))
      .getOrElse(throw new RuntimeException(s"profile '$name' not found in ${list.size} profile(s)"))
  }

  def score(p: Profile, text: String, typ: String, date: String, value: Option[Double], country: String,
            overrideV: String, today: LocalDate): Map[String, Any] = {
    val t = text.toLowerCase
    val hits = p.include.filter(t.contains)
    val bad = p.exclude.filter(t.contains)
    val kwFit = if (bad.nonEmpty) 0.0 else if (p.include.isEmpty || hits.nonEmpty) 1.0 else 0.3
    val band = value.flatMap(v => p.valueBands.find(b => v >= b._1).map(_._2))
    val countryOk = country.isEmpty || p.countries.isEmpty || p.countries.contains(country)
    val fit = (if (countryOk) 1.0 else 0.0) * kwFit * band.getOrElse(1.0)
    val weight = p.weights.getOrElse(typ, p.weights.getOrElse("other", 0.5))
    val age = Try(ChronoUnit.DAYS.between(LocalDate.parse(date.take(10)), today).toDouble).toOption.map(a => math.max(0.0, a))
    val fresh = age.map(a => math.max(0.2, 1.0 - a / (2 * p.freshnessDays))).getOrElse(0.6)
    val sc = math.round(100 * fit * weight * fresh * 10) / 10.0
    val gate0 = if (sc >= p.minScore) "pass" else "reject"
    val gate = overrideV match {
      case "include" | "pass" | "yes" => "override-pass"
      case "exclude" | "reject" | "no" => "override-reject"
      case _ => gate0
    }
    val why = Seq(
      if (hits.nonEmpty) Some("match: " + hits.take(5).mkString(", ")) else if (p.include.nonEmpty) Some("no keyword match") else None,
      if (bad.nonEmpty) Some("excluded by: " + bad.take(3).mkString(", ")) else None,
      if (!countryOk) Some(s"country $country outside ICP") else None,
      Some(f"$typ ×$weight%.2f".replace(',', '.')),
      age.map(a => s"${a.toLong} days old").orElse(Some("no date")),
      band.map(b => f"value band ×$b%.2f".replace(',', '.')),
      if (gate.startsWith("override")) Some("manual override") else None
    ).flatten.mkString(" · ")
    Map("icp_profile" -> p.name, "icp_score" -> sc, "icp_fit" -> math.round(fit * 100) / 100.0, "icp_gate" -> gate,
      "icp_reason" -> why, "icp_roles" -> p.roles.mkString(", "))
  }
}
