data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 5
  upper   = false
  special = false
  numeric = true
}

locals {
  suffix = random_string.suffix.result
  tags = {
    project     = "at-pipeline"
    environment = var.environment
    owner       = var.owner
    managed_by  = "terraform"
  }
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${var.project}-${var.environment}"
  location = var.location
  tags     = local.tags
}

# --- Storage (ADLS Gen2): durable buffer for EH Capture + medallion layers ---

resource "azurerm_storage_account" "lake" {
  name                     = "${var.project}${var.environment}${local.suffix}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # ADLS Gen2

  shared_access_key_enabled = true
  min_tls_version           = "TLS1_2"

  tags = local.tags
}

resource "azurerm_storage_container" "lake" {
  for_each              = toset(["capture", "bronze", "silver", "gold", "checkpoints"])
  name                  = each.key
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# --- Event Hubs: Kafka-compatible, with Capture writing through to ADLS ---

resource "azurerm_eventhub_namespace" "main" {
  name                 = "evhns-${var.project}-${var.environment}-${local.suffix}"
  resource_group_name  = azurerm_resource_group.main.name
  location             = azurerm_resource_group.main.location
  sku                  = "Standard"
  capacity             = 1
  auto_inflate_enabled = false

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# Let EH's managed identity write capture files into the storage account.
# Capture without this fails silently — files never appear.
resource "azurerm_role_assignment" "eh_to_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_eventhub_namespace.main.identity[0].principal_id
}

resource "azurerm_eventhub" "topic" {
  for_each          = toset(var.eventhubs)
  name              = each.key
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = 2
  message_retention = 1 # days. Capture handles long-term durability.

  capture_description {
    enabled             = true
    encoding            = "Avro"
    interval_in_seconds = 300
    size_limit_in_bytes = 314572800 # 300 MB
    skip_empty_archives = true

    destination {
      name                = "EventHubArchive.AzureBlockBlob"
      blob_container_name = azurerm_storage_container.lake["capture"].name
      storage_account_id  = azurerm_storage_account.lake.id
      archive_name_format = "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
    }
  }

  depends_on = [azurerm_role_assignment.eh_to_storage]
}

# Single Send+Listen rule for the producer. We hand the connection string to
# Key Vault; nothing else should hold it.
resource "azurerm_eventhub_namespace_authorization_rule" "producer" {
  name                = "producer"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  listen              = true
  send                = true
  manage              = false
}

# --- Key Vault: connection strings + AT API key ---

resource "azurerm_key_vault" "main" {
  name                       = "kv-${var.project}-${var.environment}-${local.suffix}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = azurerm_resource_group.main.location
  sku_name                   = "standard"
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days = 7
  purge_protection_enabled   = false # dev only.
  rbac_authorization_enabled = true

  tags = local.tags
}

resource "azurerm_role_assignment" "kv_admin" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_key_vault_secret" "eh_connection_string" {
  name         = "eventhubs-connection-string"
  value        = azurerm_eventhub_namespace_authorization_rule.producer.primary_connection_string
  key_vault_id = azurerm_key_vault.main.id

  depends_on = [azurerm_role_assignment.kv_admin]
}

resource "azurerm_key_vault_secret" "at_api_key" {
  count        = var.at_api_key == "" ? 0 : 1
  name         = "at-api-key"
  value        = var.at_api_key
  key_vault_id = azurerm_key_vault.main.id

  depends_on = [azurerm_role_assignment.kv_admin]
}
