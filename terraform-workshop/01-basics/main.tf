terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = "terraform-lab-ricardo"
  region  = "us-central1"
}

resource "google_storage_bucket" "data_lake_bucket" {
  name          = "terraform-demo-bucket-ricardo-001"
  location      = "US"
  force_destroy = true

  uniform_bucket_level_access = true
}

resource "google_storage_bucket_object" "bronze_layer" {
  name    = "bronze/"
  bucket  = google_storage_bucket.data_lake_bucket.name
  content = " "
}

resource "google_storage_bucket_object" "silver_layer" {
  name    = "silver/"
  bucket  = google_storage_bucket.data_lake_bucket.name
  content = " "
}

resource "google_storage_bucket_object" "gold_layer" {
  name    = "gold/"
  bucket  = google_storage_bucket.data_lake_bucket.name
  content = " "
}