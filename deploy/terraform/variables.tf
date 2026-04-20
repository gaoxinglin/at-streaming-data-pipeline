variable "location" {
  description = "Azure region. Keep everything in one region — cross-region egress is what kills small budgets."
  type        = string
  default     = "newzealandnorth"
}

variable "project" {
  description = "Short project name. Goes into resource names (storage account name caps at 24 chars, lowercase alphanumeric)."
  type        = string
  default     = "atpipe"

  validation {
    condition     = can(regex("^[a-z0-9]{3,10}$", var.project))
    error_message = "project must be 3-10 lowercase alphanumerics."
  }
}

variable "environment" {
  description = "Environment tag (dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "owner" {
  description = "Owner identifier (email or handle) for cost-attribution tags."
  type        = string
}

variable "eventhubs" {
  description = "Event hubs / Kafka topics to create in the namespace."
  type        = list(string)
  default = [
    "vehicle_positions",
    "trip_updates",
    "service_alerts",
    "at_alerts",
  ]
}

variable "at_api_key" {
  description = "Auckland Transport API key. If non-empty, written to Key Vault as `at-api-key`."
  type        = string
  sensitive   = true
  default     = ""
}

variable "budget_amount" {
  description = "Monthly subscription budget cap in USD. Alerts fire before you blow through it."
  type        = number
  default     = 50
}

variable "github_username" {
  description = "GitHub username for Databricks Repo checkout and git credential."
  type        = string
  default     = "gaoxinglin"
}

variable "github_pat" {
  description = "GitHub Personal Access Token for private repo access. Set to empty string if repo is public."
  type        = string
  sensitive   = true
  default     = ""
}
