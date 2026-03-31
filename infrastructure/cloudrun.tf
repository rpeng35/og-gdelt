locals {
  registry_name = "${local.name_prefix}-api"
  cloud_run_anme = "${local.name_prefix}-cloudrun-api"
}

data "google_artifact_registry_repository" "gdelt_api" {
  location      = var.gcp_region
  repository_id = local.registry_name
}

data "google_artifact_registry_docker_image" "gdelt_api" {
  location      = data.google_artifact_registry_repository.gdelt_api.location
  repository_id = data.google_artifact_registry_repository.gdelt_api.repository_id
  image_name    = var.latest_cloudrun_image_name
}

resource "google_cloud_run_service" "gdelt_api" {
  name     = local.cloud_run_anme
  location = var.gcp_region

  template {
    spec {
      containers {
        image = data.google_artifact_registry_docker_image.gdelt_api.image_uri
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }
}