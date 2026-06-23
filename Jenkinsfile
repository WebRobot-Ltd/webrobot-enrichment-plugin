// CI for the standalone SDK enrichment plugin — compiles the thin jar (against the PUBLISHED
// webrobot-plugin-sdk) and DEPLOYS it so the ETL runtime can load it, mirroring how the main ETL
// Jenkinsfile ships :example-plugin: upload jar + manifest to MinIO jars/<buildType>/plugins/ and
// register via the marketplace REST API (PluginDiscovery → adapter ServiceLoader picks up the
// WSourceStage/WPartitionStage). Separate from the main ETL build (the SDK plugin is decoupled).
//
// Secrets come from Jenkins credentials (NOT hardcoded): map these credential IDs to the same
// values the ETL job uses — github-packages-token, minio-access-key, minio-secret-key,
// webrobot-api-admin-key.
pipeline {
    agent {
        kubernetes {
            yaml '''
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: gradle-playwright
    image: ghcr.io/webrobot-ltd/jenkins-agent-web:latest
    command: ["sleep"]
    args: ["infinity"]
    tty: true
    resources:
      requests: { cpu: "500m", memory: "1500Mi" }
      limits:   { cpu: "2",    memory: "3Gi" }
'''
        }
    }

    parameters {
        choice(name: 'BUILD_TYPE', choices: ['development', 'production'], description: 'Plugin channel (MinIO jars/<BUILD_TYPE>/plugins/)')
        booleanParam(name: 'ENABLE_PLUGIN', defaultValue: false, description: 'Register the plugin as enabled (auto-load) — false = upload only')
        // API admin key as a PARAMETER (not a credential) so a missing value never fails the binding;
        // registration is optional — empty = build + upload only (register later via the marketplace API).
        password(name: 'API_ADMIN_KEY', defaultValue: '', description: 'super-admin API key to auto-register/enable the plugin (optional)')
    }

    environment {
        PLUGIN_ID      = 'webrobot-enrichment-plugin'
        PLUGIN_VERSION = '0.1.0'
        // Jenkins credentials only — NEVER hardcode secrets (this repo is PUBLIC).
        GITHUB = credentials('github-token')   // _USR = actor, _PSW = token (resolves the published SDK)
        MINIO  = credentials('minio-creds')    // _USR = access key, _PSW = secret (plugin jar store)
        MINIO_ENDPOINT = 'https://s3.metaglobe.finance'
        MINIO_BUCKET   = 'sparklogs-data'
        MINIO_ALIAS    = 'minio-local'
        API_ENDPOINT   = 'http://webrobot-jersey-api-service.webrobot.svc.cluster.local:8020'
    }

    stages {
        stage('Build jar') {
            steps {
                container('gradle-playwright') {
                    sh '''
                        echo "📦 Building ${PLUGIN_ID}-${PLUGIN_VERSION} against published webrobot-plugin-sdk..."
                        # the gradle-playwright agent has gradle on PATH (same as the ETL Jenkinsfile) — no wrapper needed
                        GITHUB_ACTOR="${GITHUB_USR}" GITHUB_TOKEN="${GITHUB_PSW}" gradle --no-daemon clean jar
                        ls -lh build/libs/
                    '''
                }
            }
        }

        stage('Deploy to MinIO + register') {
            steps {
                container('gradle-playwright') {
                    sh '''
                        set -e
                        PLUGIN_JAR=$(ls build/libs/${PLUGIN_ID}-*.jar | head -1)
                        JAR_NAME="${PLUGIN_ID}-${BUILD_NUMBER}.jar"
                        DEST="${MINIO_ALIAS}/${MINIO_BUCKET}/jars/${BUILD_TYPE}/plugins/${JAR_NAME}"
                        JAR_S3A="s3a://${MINIO_BUCKET}/jars/${BUILD_TYPE}/plugins/${JAR_NAME}"
                        MANIFEST_S3A="s3a://${MINIO_BUCKET}/jars/${BUILD_TYPE}/plugins/${PLUGIN_ID}-manifest.json"

                        # mc client
                        [ -x /tmp/mc ] || ( wget -qO /tmp/mc https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x /tmp/mc )
                        /tmp/mc alias set ${MINIO_ALIAS} ${MINIO_ENDPOINT} ${MINIO_USR} ${MINIO_PSW} --insecure

                        echo "⬆️  Uploading ${PLUGIN_JAR} → ${DEST}"
                        /tmp/mc cp "${PLUGIN_JAR}" "${DEST}" --insecure

                        # Manifest (metadata; the stages themselves are discovered from the jar via ServiceLoader)
                        cat > /tmp/manifest.json << EOF
{
  "pluginId": "${PLUGIN_ID}",
  "version": "${PLUGIN_VERSION}",
  "description": "Free public-data enrichment stages: landRegistry (UK sold-price comparables), gdelt (news tone)",
  "buildNumber": ${BUILD_NUMBER},
  "buildType": "${BUILD_TYPE}",
  "stages": ["landRegistry", "gdelt"],
  "enabled": ${ENABLE_PLUGIN}
}
EOF
                        /tmp/mc cp /tmp/manifest.json "${MINIO_ALIAS}/${MINIO_BUCKET}/jars/${BUILD_TYPE}/plugins/${PLUGIN_ID}-manifest.json" --insecure

                        # Register with the marketplace — SAME endpoint/payload the ETL Jenkinsfile uses
                        # for example-plugin: POST /api/webrobot/api/admin/plugin-installations. The runtime
                        # then resolves the jar (PluginDiscovery) and the adapter ServiceLoader loads the
                        # WSourceStage/WPartitionStage. spark_mixed = legacy registry + SDK adapter.
                        if [ -n "${API_ADMIN_KEY}" ]; then
                          cat > /tmp/payload.json << EOF
{
  "pluginId": "${PLUGIN_ID}",
  "pluginType": "spark_mixed",
  "buildType": "${BUILD_TYPE}",
  "buildNumber": ${BUILD_NUMBER},
  "version": "${PLUGIN_VERSION}",
  "jarPath": "${JAR_S3A}",
  "manifestPath": "${MANIFEST_S3A}",
  "enabled": ${ENABLE_PLUGIN},
  "organizationId": null,
  "description": "Free public-data enrichment: landRegistry (UK sold prices) + gdelt (news tone) - Build ${BUILD_NUMBER}"
}
EOF
                          echo "🔌 Registering via ${API_ENDPOINT}/api/webrobot/api/admin/plugin-installations ..."
                          HTTP_CODE=$(curl -s -X POST "${API_ENDPOINT}/api/webrobot/api/admin/plugin-installations" \
                            -H "X-API-Key: ${API_ADMIN_KEY}" -H "Content-Type: application/json" \
                            -d @/tmp/payload.json -o /tmp/resp.json -w "%{http_code}")
                          echo "HTTP ${HTTP_CODE}: $(cat /tmp/resp.json 2>/dev/null)"
                          if [ "${HTTP_CODE}" = "200" ] || [ "${HTTP_CODE}" = "201" ]; then
                            PLUGIN_DB_ID=$(grep -o '"id":[^,}]*' /tmp/resp.json | head -1 | cut -d':' -f2 | tr -d ' ')
                            if [ "${ENABLE_PLUGIN}" = "true" ] && [ -n "${PLUGIN_DB_ID}" ]; then
                              curl -s -X POST "${API_ENDPOINT}/webrobot/api/admin/plugin-installations/${PLUGIN_DB_ID}/enable" \
                                -H "X-API-Key: ${API_ADMIN_KEY}" -w "\nenable HTTP %{http_code}\n" || true
                            fi
                          else
                            echo "⚠️ registration failed (jar+manifest ARE uploaded — register/enable via the marketplace API later)"
                          fi
                        else
                          echo "ℹ️ No API key — jar+manifest uploaded; register/enable via the marketplace API later."
                        fi
                        echo "✅ ${JAR_S3A}"
                    '''
                }
            }
        }
    }

    post {
        success { echo "✅ ${env.PLUGIN_ID} built + deployed to jars/${params.BUILD_TYPE}/plugins/ (enabled=${params.ENABLE_PLUGIN})" }
        always  { archiveArtifacts artifacts: 'build/libs/*.jar', allowEmptyArchive: true }
    }
}
