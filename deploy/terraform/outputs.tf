output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "abfss_paths" {
  description = "ABFSS base paths per container, ready to drop into Spark/Databricks configs."
  value = {
    for name, c in azurerm_storage_container.lake :
    name => "abfss://${c.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
  }
}

output "eventhubs_namespace" {
  value = azurerm_eventhub_namespace.main.name
}

output "eventhubs_kafka_bootstrap" {
  description = "Use as KAFKA_BOOTSTRAP_SERVERS (SASL_SSL on 9093)."
  value       = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL. Set as DATABRICKS_HOST in .env."
  value       = "https://${azurerm_databricks_workspace.main.workspace_url}"
}

output "databricks_sp_client_id" {
  description = "Service principal client ID for Spark OAuth config."
  value       = azuread_application.databricks.client_id
}

output "next_steps" {
  description = "What to do after apply."
  value       = <<-EOT

    Phase 1 (Event Hubs + Storage):
    1. Pull producer connection string:
         az keyvault secret show --vault-name ${azurerm_key_vault.main.name} \
           --name eventhubs-connection-string --query value -o tsv

    Phase 2 (Databricks):
    2. Open workspace: https://${azurerm_databricks_workspace.main.workspace_url}

    3. Generate a PAT token in workspace UI:
         User Settings → Developer → Access Tokens → Generate new token
       Then store it:
         az keyvault secret set --vault-name ${azurerm_key_vault.main.name} \
           --name databricks-pat-token --value <token>

    4. Create Secret Scope backed by Key Vault:
         databricks secrets create-scope at-pipeline \
           --scope-backend-type AZURE_KEYVAULT \
           --resource-id $(az keyvault show --name ${azurerm_key_vault.main.name} --query id -o tsv) \
           --dns-name https://${azurerm_key_vault.main.name}.vault.azure.net/

    5. Set env vars for dbt prod target:
         DATABRICKS_HOST=https://${azurerm_databricks_workspace.main.workspace_url}
         DATABRICKS_TOKEN=<PAT from step 3>
         DATABRICKS_HTTP_PATH=<SQL Warehouse http_path — from workspace UI>
  EOT
}
