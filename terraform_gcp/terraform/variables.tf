locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "clever-grid-339211"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "asia-south2"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "BigQuery Table "
  type = string
  default = "ny_trips"
}