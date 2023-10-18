variable "region" {
  description = "The region to deploy the resources in."
  default     = "us-central1"
}

variable "bucket" {
  description = "The name of the staging bucket."
  default     = "railway_station"
}

variable "network" {
  description = "The name of the network."
}

variable "zone" {
  description = "The zone to deploy the resources in."
  default     = "us-central1-a"
}

variable "credentials" {
  description = "The path to the Google Cloud credentials file."
  default     = "/home/stefen/train/terraform/your_credentials.json"
}

variable "project" {
  description = "The ID of the Google Cloud project."
  default     = ""
}


variable "stg_bq_dataset" {
  description = "The ID of the staging BigQuery dataset."
  type        = string
}

variable "prod_bq_dataset" {
  description = "The ID of the production BigQuery dataset."
  type        = string
}
