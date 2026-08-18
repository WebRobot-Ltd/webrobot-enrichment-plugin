// WebRobot Enrichment Plugin — FREE / open-source ETL stages that enrich scraped data with public,
// no-API-key data sources (HM Land Registry sold prices, GDELT news tone). Built on the WebRobot
// plugin SDK only — no ETL internals — so it is fully decoupled and publishable on GitHub.
plugins {
    id("scala")
    id("java-library")
}

group   = "eu.webrobot.plugins.enrichment"
version = "0.1.0"

repositories {
    mavenCentral()
    maven {
        name = "GitHubPackages"
        url  = uri("https://maven.pkg.github.com/WebRobot-Ltd/webrobot-etl")
        credentials {
            username = System.getenv("GITHUB_ACTOR") ?: "webroboteu"
            password = System.getenv("GITHUB_TOKEN")
                ?: throw GradleException("GITHUB_TOKEN env var required to download webrobot-plugin-sdk")
        }
    }
}

val scalaV     = "2.12"
val scalaFullV = "2.12.18"
// pinned to the published SDK version (GitHub Packages WebRobot-Ltd/webrobot-etl) — avoids the
// maven-metadata lookup that `latest.release` needs (and which a read-only token may not see).
val sdkVersion = "0.9.0.4"

dependencies {
    // The ONLY dependency — the WebRobot plugin SDK. No ETL internals, no SpookyStuff, no JSON lib
    // (we parse the two well-formed public APIs with regex to keep the jar truly standalone).
    compileOnly("eu.webrobot:webrobot-plugin-sdk:$sdkVersion")
    compileOnly("org.scala-lang:scala-library:$scalaFullV")

    testImplementation("org.scalatest:scalatest_$scalaV:3.2.18")
    testImplementation("org.scala-lang:scala-library:$scalaFullV")
}

tasks.withType<Jar> {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// ─────────────────────────────────────────────────────────────────────────────
// Manifest del plugin
//
// La piattaforma popola il catalogo degli stage — quello che alimenta il Pipeline Designer e i
// suggerimenti in chat — leggendo questo manifest all'abilitazione del plugin (StageSpecSync).
// Finche' il manifest non esiste, il plugin risulta installato e attivo ma i suoi stage non li vede
// nessuno: un catalogo incompleto non da' errore, semplicemente non mostra nulla.
//
// stage-specs.json e' generato da tools/generate_stage_specs.py leggendo le sorgenti Scala, cosi'
// descrizioni e argomenti non divergono dal codice. Il controllo qui sotto e' la rete di sicurezza:
// se qualcuno aggiunge uno stage senza rigenerare, la build si ferma.
// ─────────────────────────────────────────────────────────────────────────────

val manifestFile = layout.buildDirectory.file("webrobot-enrichment-plugin-manifest.json")

// Stage deliberatamente non pubblicati: impalcatura per collaudare il ponte RDD dell'SDK.
// Deve restare allineata a NON_PUBBLICATI in tools/generate_stage_specs.py.
val stageNonPubblicati = setOf("rowMultiply", "filterGt", "explodeCsv", "sumByKey", "countByKey")

tasks.register("generateManifest") {
    description = "Assembla il manifest del plugin dagli stage dichiarati in stage-specs.json."
    group = "build"
    val specs = file("src/main/resources/stage-specs.json")
    val sorgenti = fileTree("src/main/scala") { include("**/*.scala") }
    inputs.file(specs)
    inputs.files(sorgenti)
    outputs.file(manifestFile)

    doLast {
        if (!specs.exists()) {
            throw GradleException("stage-specs.json mancante — esegui tools/generate_stage_specs.py")
        }

        // Rete di sicurezza: ogni `override def name` nelle sorgenti dev'essere o dichiarato nel
        // manifest o escluso di proposito. Un nuovo stage dimenticato sparirebbe in silenzio.
        val nelCodice = sorgenti.files
            .flatMap { Regex("""override\s+def\s+name:\s*String\s*=\s*"([^"]+)"""").findAll(it.readText()).map { m -> m.groupValues[1] } }
            .toSet()
        val testoSpecs = specs.readText()
        val dichiarati = Regex(""""stage_name"\s*:\s*"([^"]+)"""").findAll(testoSpecs).map { it.groupValues[1] }.toSet()
        val mancanti = nelCodice - dichiarati - stageNonPubblicati
        if (mancanti.isNotEmpty()) {
            throw GradleException(
                "Stage presenti nel codice ma assenti dal manifest: ${mancanti.sorted().joinToString(", ")}. " +
                "Rigenera con: python3 tools/generate_stage_specs.py > src/main/resources/stage-specs.json"
            )
        }
        val fantasma = dichiarati - nelCodice
        if (fantasma.isNotEmpty()) {
            throw GradleException("Stage dichiarati nel manifest ma inesistenti nel codice: ${fantasma.sorted().joinToString(", ")}")
        }

        val out = manifestFile.get().asFile
        out.parentFile.mkdirs()
        out.writeText(
            """
            {
              "pluginId": "webrobot-enrichment-plugin",
              "version": "$version",
              "pluginType": "spark_mixed",
              "displayName": "WebRobot Enrichment",
              "description": "Stage di arricchimento su fonti dati pubbliche e gratuite: nessuna chiave API richiesta.",
              "enabled": true,
              "stages": $testoSpecs
            }
            """.trimIndent()
        )
        logger.lifecycle("Manifest scritto: ${out.absolutePath} (${dichiarati.size} stage pubblicati, ${stageNonPubblicati.size} esclusi)")
    }
}

tasks.named("build") { dependsOn("generateManifest") }
