resource "google_workflows_workflow" "workflows_state" {  
  name          = "workflow-ingestion-terraform"
  region        = "europe-west9"
  description   = ""
  service_account = "projects/simplon-dev-437314/serviceAccounts/simlplon-dev-sr@simplon-dev-437314.iam.gserviceaccount.com"
  labels = {
    type = "ingestion"
  }
  #user_env_vars = {
  #  url = "https://timeapi.io/api/Time/current/zone?timeZone=Etc/UTC"
  #}
  source_contents = <<-EOF
---
main:
  params: [input]
  steps:
    - Ingestion:
        call: http.post
        args:
          url: https://csv-to-parquet-1023144124721.europe-west9.run.app
          auth:
            type: OIDC
        result: IngestionJson
    - ReturnOutput:
        return: $${IngestionJson}
EOF
}