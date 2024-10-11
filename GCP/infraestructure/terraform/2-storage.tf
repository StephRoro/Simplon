resource "google_storage_bucket" "function_bucket" {
    name     = "simplon-dev-ingestion-sr-terraform"
    location = var.region
}

resource "google_storage_bucket" "function_bucket_2" {
    name     = "simplon-dev-consuption-sr-terraform"
    location = var.region
}

resource "google_storage_bucket" "function_bucket_3" {
    name     = "simplon-dev-processing-sr-terraform"
    location = var.region
}

resource "google_storage_bucket" "function_bucket_4" {
    name     = "simplon-dev-scripts-and-configs-sr-terraform"
    location = var.region
}

resource "google_storage_bucket" "function_bucket_5" {
    name     = "simplon-cloud-function-terraform"
    location = var.region
}
