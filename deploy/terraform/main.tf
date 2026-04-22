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
  message_retention = 7

  capture_description {
    enabled             = false
    encoding            = "Avro"
    interval_in_seconds = 300
    size_limit_in_bytes = 314572800
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

# --- Container Registry ---

resource "azurerm_container_registry" "main" {
  name                = "acr${var.project}${var.environment}${local.suffix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "Basic"
  admin_enabled       = false
  tags                = local.tags
}

# --- AT Producer: Azure Container Instance ---

resource "azurerm_user_assigned_identity" "producer" {
  name                = "id-${var.project}-${var.environment}-producer"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.tags
}

resource "azurerm_role_assignment" "producer_acr_pull" {
  scope                = azurerm_container_registry.main.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.producer.principal_id
}

resource "azurerm_container_group" "producer" {
  name                = "aci-${var.project}-${var.environment}-producer"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  ip_address_type     = "None"
  os_type             = "Linux"
  restart_policy      = "Always"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.producer.id]
  }

  image_registry_credential {
    server                    = azurerm_container_registry.main.login_server
    user_assigned_identity_id = azurerm_user_assigned_identity.producer.id
  }

  container {
    name   = "producer"
    image  = "${azurerm_container_registry.main.login_server}/at-producer:latest"
    cpu    = "0.25"
    memory = "0.5"

    environment_variables = {
      KAFKA_BOOTSTRAP_SERVERS = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
    }

    secure_environment_variables = {
      AT_API_KEY                  = var.at_api_key
      EVENTHUBS_CONNECTION_STRING = azurerm_eventhub_namespace_authorization_rule.producer.primary_connection_string
    }
  }

  tags       = local.tags
  depends_on = [azurerm_role_assignment.producer_acr_pull]
}

# --- Databricks networking (VNet injection, no NAT gateway) ---
# Workers get public IPs directly (no_public_ip = false), so no NAT gateway needed.
# This costs ~$32/month less than the auto-managed VNet that Databricks creates.

resource "azurerm_virtual_network" "databricks" {
  name                = "workers-vnet"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  address_space       = ["10.179.0.0/16"]
  tags                = local.tags
}

resource "azurerm_network_security_group" "databricks" {
  name                = "workers-sg"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  tags                = local.tags
}

resource "azurerm_subnet" "databricks_public" {
  name                 = "public"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.databricks.name
  address_prefixes     = ["10.179.0.0/18"]

  delegation {
    name = "databricks-del"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

resource "azurerm_subnet" "databricks_private" {
  name                 = "private"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.databricks.name
  address_prefixes     = ["10.179.64.0/18"]

  delegation {
    name = "databricks-del"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

resource "azurerm_subnet_network_security_group_association" "databricks_public" {
  subnet_id                 = azurerm_subnet.databricks_public.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

resource "azurerm_subnet_network_security_group_association" "databricks_private" {
  subnet_id                 = azurerm_subnet.databricks_private.id
  network_security_group_id = azurerm_network_security_group.databricks.id
}

# --- Databricks Access Connector (Unity Catalog external location auth) ---

resource "azurerm_databricks_access_connector" "main" {
  name                = "ac-${var.project}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.databricks_location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "ac_to_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.main.identity[0].principal_id
}

# --- Databricks workspace ---

resource "azurerm_databricks_workspace" "main" {
  name                        = "dbw-${var.project}-${var.environment}-${local.suffix}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = var.databricks_location  # Databricks not available in newzealandnorth
  sku                         = "premium"
  managed_resource_group_name = "rg-${var.project}-${var.environment}-managed"

  custom_parameters {
    no_public_ip                                         = false
    virtual_network_id                                   = azurerm_virtual_network.databricks.id
    public_subnet_name                                   = azurerm_subnet.databricks_public.name
    private_subnet_name                                  = azurerm_subnet.databricks_private.name
    public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.databricks_public.id
    private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.databricks_private.id
  }

  tags = local.tags
}

# Service principal so Databricks clusters can authenticate to Azure resources
# via OAuth (no storage keys, no HMAC). Credentials rotate on re-apply.
resource "azuread_application" "databricks" {
  display_name = "sp-${var.project}-${var.environment}-databricks"
}

resource "azuread_service_principal" "databricks" {
  client_id = azuread_application.databricks.client_id
}

resource "azuread_service_principal_password" "databricks" {
  service_principal_id = azuread_service_principal.databricks.id
}

# Databricks SP → read from Event Hubs (Spark Structured Streaming source)
resource "azurerm_role_assignment" "dbw_to_eh" {
  scope                = azurerm_eventhub_namespace.main.id
  role_definition_name = "Azure Event Hubs Data Receiver"
  principal_id         = azuread_service_principal.databricks.object_id
}

# Databricks SP → read/write Bronze, Silver, Gold, Checkpoints containers
resource "azurerm_role_assignment" "dbw_to_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azuread_service_principal.databricks.object_id
}

# Databricks SP → read secrets from Key Vault (used by Databricks secret scope)
resource "azurerm_role_assignment" "dbw_to_kv" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azuread_service_principal.databricks.object_id
}

# Store SP credentials + workspace URL in KV for Databricks secret scope + dbt/CI
resource "azurerm_key_vault_secret" "databricks_host" {
  name         = "databricks-host"
  value        = "https://${azurerm_databricks_workspace.main.workspace_url}"
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.kv_admin]
}

resource "azurerm_key_vault_secret" "sp_tenant_id" {
  name         = "databricks-sp-tenant-id"
  value        = data.azurerm_client_config.current.tenant_id
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.kv_admin]
}

resource "azurerm_key_vault_secret" "sp_client_id" {
  name         = "databricks-sp-client-id"
  value        = azuread_application.databricks.client_id
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.kv_admin]
}

resource "azurerm_key_vault_secret" "sp_client_secret" {
  name         = "databricks-sp-client-secret"
  value        = azuread_service_principal_password.databricks.value
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.kv_admin]
}

# --- Budget: subscription-scoped guard rail ---
# `az consumption budget create` is stuck on a pre-2019 API shape and 400s with
# "use filter interface". Terraform hits the current ARM API directly, so this
# is the path that actually works. Alerts go to var.owner — assumes it's an
# email, which it is in practice.

resource "azurerm_consumption_budget_subscription" "monthly" {
  name            = "${var.project}-${var.environment}-monthly"
  subscription_id = "/subscriptions/${data.azurerm_client_config.current.subscription_id}"

  amount     = var.budget_amount
  time_grain = "Monthly"

  # Start at the first of the current month. End_date omitted → Azure defaults
  # to 10 years out, which is fine; budget can be edited in place.
  time_period {
    start_date = "2026-04-01T00:00:00Z"
  }

  # Forecast trips early so you have time to react; actual at 100% is the cap.
  notification {
    enabled        = true
    threshold      = 80
    operator       = "GreaterThan"
    threshold_type = "Forecasted"
    contact_emails = [var.owner]
  }

  notification {
    enabled        = true
    threshold      = 100
    operator       = "GreaterThan"
    threshold_type = "Actual"
    contact_emails = [var.owner]
  }
}
