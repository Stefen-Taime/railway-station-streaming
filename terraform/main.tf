provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "dataproc_staging_bucket" {
  name          = var.bucket
  location      = "US"
  force_destroy = true
}

resource "google_dataproc_cluster" "mulitnode_spark_cluster" {
  name   = "railway-station-spark-cluster"
  region = var.region

  cluster_config {
    staging_bucket = google_storage_bucket.dataproc_staging_bucket.name

    gce_cluster_config {
      network = "default"  
      zone    = var.zone

      shielded_instance_config {
        enable_secure_boot = true
      }
    }

    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"   
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 50  
      }
    }

    worker_config {
      num_instances = 4  
      machine_type  = "n1-standard-4" 
      disk_config {
        boot_disk_size_gb = 50 
      }
    }

    software_config {
      image_version = "2.0-debian10"
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
      optional_components = ["JUPYTER"]
    }
  }
}

resource "google_bigquery_dataset" "stg_dataset" {
  dataset_id                 = var.stg_bq_dataset
  project                    = var.project
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "prod_dataset" {
  dataset_id                 = var.prod_bq_dataset
  project                    = var.project
  location                   = var.region
  delete_contents_on_destroy = true
}
