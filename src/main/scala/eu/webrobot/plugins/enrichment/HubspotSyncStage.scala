package eu.webrobot.plugins.enrichment

import eu.webrobot.plugin.sdk.{WArgs, WPartitionStage, WRow, WebroStageContext}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.security.MessageDigest
import java.time.Duration
import scala.collection.mutable
import scala.util.Try

/**
 * Pushes GTM signals (tenders, hiring, funding, conversations…) into the organisation's HubSpot CRM — one output row
 * per input row, with the HubSpot ids.
 *
 * For every row: the company is upserted by `wr_key` (normalised domain, or the company name when there is no domain),
 * the deal of the signal by `wr_signal_id` and the contact, when an email is given, by email; deal, company and contact
 * are associated. A NEW deal starts in the first stage of the pipeline (default "Social selling" / "Signal"); an
 * EXISTING deal only gets its signal fields refreshed — its stage is never moved back, so the sales team's work
 * survives every rerun. Works in batches of 100 (HubSpot batch APIs) and retries on 429.
 *
 * The token is the org's HUBSPOT cloud credential (a private-app token), injected by the platform as HUBSPOT_API_KEY —
 * never in the pipeline. The portal needs the wr_* properties and the pipeline: see hubspot/setup_hubspot.py.
 *
 * Pipeline YAML:
 * {{{
 * - stage: hubspotSync
 *   args:
 *     - "company"          # company name column
 *     - "domain"           # company domain / website column ("" = none)
 *     - "signal_type"      # signal type column: tender, hiring, funding, registry, conversation, engagement, inbound
 *     - "signal_url"       # link to the signal
 *     - "signal_date"      # date of the signal (YYYY-MM-DD…)
 *     - "signal_summary"   # one-line summary column
 *     - ""                 # contact email column ("" = no contact)
 *     - "default"          # ICP profile name written on company, deal and contact
 *     - "icp_score"        # ICP score column, 0-100 ("" = none)
 *     - "webrobot"         # source label (pipeline / dataset name)
 *     - "Social selling"   # deal pipeline
 *     - "Signal"           # stage of new deals
 * }}}
 * Adds hs_company_id, hs_deal_id, hs_contact_id, hs_deal_url, hs_status (created / updated / error:…).
 */
class HubspotSyncStage extends WPartitionStage {

  override def name: String = "hubspotSync"

  override def transformPartition(rows: Iterator[WRow], args: WArgs, ctx: WebroStageContext): Iterator[WRow] = {
    val companyF  = args.string(0, "company")
    val domainF   = args.string(1, "domain")
    val typeF     = args.string(2, "signal_type")
    val urlF      = args.string(3, "signal_url")
    val dateF     = args.string(4, "signal_date")
    val summaryF  = args.string(5, "signal_summary")
    val emailF    = args.string(6, "")
    val icp       = args.string(7, "default")
    val scoreF    = args.string(8, "icp_score")
    val source    = args.string(9, "webrobot")
    val pipeName  = args.string(10, "Social selling")
    val stageName = args.string(11, "Signal")
    val all = rows.toList
    if (all.isEmpty) return Iterator.empty
    val token = Option(System.getenv("HUBSPOT_API_KEY")).filter(_.nonEmpty)
      .getOrElse(ctx.config("spark.executorEnv.HUBSPOT_API_KEY", ""))
    if (token.isEmpty)
      return all.iterator.map(r => Enrichers.augment(r, Map("hs_status" -> "error:no HUBSPOT credential for this organisation")))
    val hs = new Hubspot.Client(token, ctx)
    val (pipeId, stageId, portal) = Try(hs.pipeline(pipeName, stageName)).recover { case e: Throwable =>
      ctx.warn(s"hubspotSync: ${e.getMessage}"); ("", "", "")
    }.get
    if (pipeId.isEmpty)
      return all.iterator.map(r => Enrichers.augment(r, Map("hs_status" -> s"error:pipeline '$pipeName' / stage '$stageName' not found")))

    def s(r: WRow, f: String): String =
      if (f.isEmpty) "" else Option(r.fields.getOrElse(f, "")).map(String.valueOf).filter(v => v != "null").getOrElse("").trim
    all.grouped(100).flatMap { batch =>
      val sigs = batch.map { r =>
        val company = s(r, companyF)
        val domain  = Hubspot.domainOf(s(r, domainF))
        val key     = if (domain.nonEmpty) domain else if (company.nonEmpty) "name:" + Hubspot.slug(company) else ""
        val typ     = Some(s(r, typeF).toLowerCase).filter(Hubspot.TYPES.contains).getOrElse("other")
        val url     = s(r, urlF)
        val date    = Some(s(r, dateF)).filter(_.matches("""\d{4}-\d{2}-\d{2}.*""")).map(_.take(10)).getOrElse("")
        val summary = s(r, summaryF).take(2000)
        val score   = if (scoreF.isEmpty) None else r.double(scoreF).filter(v => !v.isNaN)
        val sigId   = Hubspot.sha1(Seq(key, typ, if (url.nonEmpty) url else summary + date).mkString("|")).take(24)
        Hubspot.Sig(r, company, domain, key, typ, url, date, summary, s(r, emailF).toLowerCase, score, sigId)
      }
      Try(hs.sync(sigs.filter(_.key.nonEmpty), icp, source, pipeId, stageId)).map { res =>
        sigs.map { g =>
          if (g.key.isEmpty) Enrichers.augment(g.row, Map("hs_status" -> "error:no company name or domain"))
          else res.getOrElse(g.sigId, Map("hs_status" -> "error:not returned by HubSpot")) match {
            case m if m.contains("hs_deal_id") =>
              Enrichers.augment(g.row, m + ("hs_deal_url" -> s"https://app-eu1.hubspot.com/contacts/$portal/record/0-3/${m("hs_deal_id")}"))
            case m => Enrichers.augment(g.row, m)
          }
        }
      }.recover { case e: Throwable =>
        ctx.warn(s"hubspotSync batch: ${e.getMessage}")
        sigs.map(g => Enrichers.augment(g.row, Map("hs_status" -> ("error:" + String.valueOf(e.getMessage).take(150)))))
      }.get
    }
  }
}

object Hubspot {
  import SatelliteIndicesStage.Json

  val API = "https://api.hubapi.com"
  val TYPES = Set("tender", "hiring", "funding", "registry", "conversation", "engagement", "inbound", "other")

  final case class Sig(row: WRow, company: String, domain: String, key: String, typ: String, url: String, date: String,
                       summary: String, email: String, score: Option[Double], sigId: String)

  def num(v: Double): String = String.format(java.util.Locale.ROOT, "%.1f", Double.box(v))

  def sha1(s: String): String = MessageDigest.getInstance("SHA-1").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString

  def slug(s: String): String = s.toLowerCase.replaceAll("[^a-z0-9]+", "-").replaceAll("(^-|-$)", "").take(80)

  /** "https://www.Acme.com/about" → "acme.com"; "" when it does not look like a domain. */
  def domainOf(v: String): String = {
    val d = v.trim.toLowerCase.replaceFirst("^[a-z]+://", "").replaceFirst("^www\\.", "").takeWhile(c => c != '/' && c != '?' && c != '#')
    if (d.matches("""[a-z0-9.-]+\.[a-z]{2,}""")) d else ""
  }

  /** JSON string literal, control characters included. */
  def q(s: String): String = {
    val b = new StringBuilder("\"")
    s.foreach {
      case '"' => b.append("\\\""); case '\\' => b.append("\\\\"); case '\n' => b.append("\\n"); case '\r' => b.append("\\r")
      case '\t' => b.append("\\t"); case c if c < ' ' => b.append(f"\\u${c.toInt}%04x"); case c => b.append(c)
    }
    b.append('"').toString
  }

  def obj(kv: Seq[(String, String)]): String = kv.filter(_._2 != null).map { case (k, v) => q(k) + ":" + v }.mkString("{", ",", "}")

  class Client(token: String, ctx: WebroStageContext) {
    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15)).build()

    def call(method: String, path: String, body: String = null): Map[String, Any] = {
      var attempt = 0
      while (true) {
        val b = HttpRequest.newBuilder(URI.create(API + path)).timeout(Duration.ofSeconds(60))
          .header("Authorization", "Bearer " + token).header("Content-Type", "application/json")
        val req = if (body == null) b.method(method, HttpRequest.BodyPublishers.noBody()).build()
                  else b.method(method, HttpRequest.BodyPublishers.ofString(body)).build()
        val r = http.send(req, HttpResponse.BodyHandlers.ofString())
        val code = r.statusCode()
        if ((code == 429 || code >= 500) && attempt < 5) {
          attempt += 1
          Thread.sleep(math.min(30000L, 1500L * attempt * attempt))
        } else {
          val o = Try(Json.obj(Json.parse(r.body()))).getOrElse(Map.empty[String, Any])
          // 207 = batch with some failures: the caller reads "errors"
          if (code >= 300 && code != 207)
            throw new RuntimeException(s"HubSpot $method $path → HTTP $code: " + o.get("message").map(String.valueOf).getOrElse(r.body().take(200)))
          return o
        }
      }
      Map.empty
    }

    /** (pipeline id, stage id of `stageName`, portal id). */
    def pipeline(pipeName: String, stageName: String): (String, String, String) = {
      val ps = Json.arr(call("GET", "/crm/v3/pipelines/deals").get("results")).map(Json.obj)
      val p = ps.find(x => String.valueOf(x.getOrElse("label", "")).equalsIgnoreCase(pipeName))
        .getOrElse(throw new RuntimeException(s"deal pipeline '$pipeName' not found: run hubspot/setup_hubspot.py"))
      val st = Json.arr(p.get("stages")).map(Json.obj).find(x => String.valueOf(x.getOrElse("label", "")).equalsIgnoreCase(stageName))
        .getOrElse(throw new RuntimeException(s"stage '$stageName' not found in '$pipeName'"))
      val portal = Try(call("GET", "/account-info/v3/details").get("portalId").map(v => String.valueOf(Json.num(v).toLong)).getOrElse(""))
        .getOrElse("")
      (String.valueOf(p("id")), String.valueOf(st("id")), portal)
    }

    private def ids(res: Map[String, Any], keyProp: String): Map[String, String] =
      Json.arr(res.get("results")).map(Json.obj).flatMap { x =>
        val props = Json.obj(x.get("properties"))
        props.get(keyProp).map(k => String.valueOf(k) -> String.valueOf(x("id")))
      }.toMap

    def sync(sigs: Seq[Sig], icp: String, source: String, pipeId: String, stageId: String): Map[String, Map[String, Any]] = {
      if (sigs.isEmpty) return Map.empty
      val out = mutable.Map[String, Map[String, Any]]()
      // 1. companies: upsert by wr_key (last signal wins)
      val byKey = sigs.groupBy(_.key).map { case (k, gs) => k -> gs.maxBy(g => g.date + g.sigId) }
      val compIn = byKey.values.map { g =>
        obj(Seq("idProperty" -> q("wr_key"), "id" -> q(g.key), "properties" -> obj(Seq(
          "wr_key" -> q(g.key), "wr_icp_profile" -> q(icp), "wr_source" -> q(source), "wr_last_signal_type" -> q(g.typ),
          "wr_last_signal_url" -> q(g.url)) ++
          (if (g.company.nonEmpty) Seq("name" -> q(g.company)) else Nil) ++
          (if (g.domain.nonEmpty) Seq("domain" -> q(g.domain)) else Nil) ++
          (if (g.date.nonEmpty) Seq("wr_last_signal_at" -> q(g.date)) else Nil) ++
          g.score.map(v => "wr_icp_score" -> Hubspot.num(v)).toSeq)))
      }
      val compIds = ids(call("POST", "/crm/v3/objects/companies/batch/upsert", s"""{"inputs":[${compIn.mkString(",")}]}"""), "wr_key")
      // 2. deals: read the existing ones by wr_signal_id, create the new ones in the first stage, refresh the others
      val existing = ids(call("POST", "/crm/v3/objects/deals/batch/read",
        s"""{"idProperty":"wr_signal_id","properties":["wr_signal_id"],"inputs":[${sigs.map(g => obj(Seq("id" -> q(g.sigId)))).distinct.mkString(",")}]}"""),
        "wr_signal_id")
      def dealProps(g: Sig): Seq[(String, String)] = Seq(
        "wr_signal_id" -> q(g.sigId), "wr_signal_type" -> q(g.typ), "wr_signal_url" -> q(g.url),
        "wr_signal_summary" -> q(g.summary), "wr_icp_profile" -> q(icp), "wr_source" -> q(source)) ++
        (if (g.date.nonEmpty) Seq("wr_signal_date" -> q(g.date)) else Nil) ++ g.score.map(v => "wr_icp_score" -> Hubspot.num(v)).toSeq
      val uniq = sigs.groupBy(_.sigId).map(_._2.head).toList
      val (old, fresh) = uniq.partition(g => existing.contains(g.sigId))
      val created = if (fresh.isEmpty) Map.empty[String, String] else ids(call("POST", "/crm/v3/objects/deals/batch/create",
        s"""{"inputs":[${fresh.map { g =>
          val title = s"${g.typ.capitalize} · ${if (g.company.nonEmpty) g.company else g.key}" + (if (g.summary.nonEmpty) " — " + g.summary.take(80) else "")
          obj(Seq("properties" -> obj(dealProps(g) ++ Seq("dealname" -> q(title.take(250)), "pipeline" -> q(pipeId), "dealstage" -> q(stageId)))))
        }.mkString(",")}]}"""), "wr_signal_id")
      if (old.nonEmpty) call("POST", "/crm/v3/objects/deals/batch/update",
        s"""{"inputs":[${old.map(g => obj(Seq("id" -> q(existing(g.sigId)), "properties" -> obj(dealProps(g))))).mkString(",")}]}""")
      // 3. contacts (optional): upsert by email
      val withEmail = uniq.filter(_.email.contains("@"))
      val contactIds = if (withEmail.isEmpty) Map.empty[String, String] else ids(call("POST", "/crm/v3/objects/contacts/batch/upsert",
        s"""{"inputs":[${withEmail.groupBy(_.email).map(_._2.head).map(g => obj(Seq("idProperty" -> q("email"), "id" -> q(g.email),
          "properties" -> obj(Seq("email" -> q(g.email), "wr_icp_profile" -> q(icp), "wr_source" -> q(source), "wr_signal_url" -> q(g.url)))))).mkString(",")}]}"""),
        "email")
      // 4. associations (default types): deal → company, deal → contact, contact → company
      def assoc(from: String, to: String, pairs: Seq[(String, String)]): Unit = if (pairs.nonEmpty)
        Try(call("POST", s"/crm/v4/associations/$from/$to/batch/associate/default",
          s"""{"inputs":[${pairs.distinct.map { case (a, b) => s"""{"from":{"id":${q(a)}},"to":{"id":${q(b)}}}""" }.mkString(",")}]}"""))
          .failed.foreach(e => ctx.warn(s"hubspotSync associations $from→$to: ${e.getMessage}"))
      val dealOf = (g: Sig) => created.get(g.sigId).orElse(existing.get(g.sigId))
      assoc("deals", "companies", uniq.flatMap(g => for (d <- dealOf(g); c <- compIds.get(g.key)) yield (d, c)))
      assoc("deals", "contacts", withEmail.flatMap(g => for (d <- dealOf(g); c <- contactIds.get(g.email)) yield (d, c)))
      assoc("contacts", "companies", withEmail.flatMap(g => for (p <- contactIds.get(g.email); c <- compIds.get(g.key)) yield (p, c)))
      uniq.foreach { g =>
        out(g.sigId) = dealOf(g) match {
          case Some(d) => Map[String, Any]("hs_deal_id" -> d, "hs_company_id" -> compIds.getOrElse(g.key, ""),
            "hs_contact_id" -> contactIds.getOrElse(g.email, ""), "hs_status" -> (if (created.contains(g.sigId)) "created" else "updated"))
          case None => Map[String, Any]("hs_status" -> "error:deal not created")
        }
      }
      out.toMap
    }
  }
}
