terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "simplon-dev-437314"
  credentials = file("credentials/simplon-dev-437314-60cdab0ced90.json")
  region  = "europe-west9"
  zone    = "europe-west9-c"
}
