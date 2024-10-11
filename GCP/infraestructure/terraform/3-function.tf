resource "google_storage_bucket_object" "ingestion" {
    name         = "csv-to-parquet"
    bucket       = google_storage_bucket.function_bucket_5.name
    content_type = "application/zip"
    source       = "cloud-functions-files/csv-to-parquet.zip"
}

resource "google_cloudfunctions2_function" "ingestion_function2" {
    provider    =  google
    name        = "csv-to-parquet-terraform"
    location    = "europe-west9"

    build_config {
        runtime               = "python312"
        entry_point           = "csv_to_parquet"
        source {
          storage_source {        
            bucket = google_storage_bucket.function_bucket_5.name
            object = google_storage_bucket_object.ingestion.name
          }
        }
      
    }

    service_config {
        max_instance_count  = 1
        available_memory    = "512M"
        timeout_seconds     = 900
    }

}
