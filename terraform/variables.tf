# all variables are set through TF_VAR_
variable "impersonate_sa" {
  description = "Service account to impersonate (set via TF_VAR_impersonate_sa in .env)"
}

variable "project" {
  description = "GCP project ID"
}

variable "region" {
  description = "GCP region"
}

variable "location" {
  description = "GCP location (for GCS and BQ)"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
}

variable "gcs_bucket_name" {
  description = "GCS bucket name (must be globally unique)"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"

}