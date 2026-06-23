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
