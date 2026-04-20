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

output "next_steps" {
  description = "What to do after apply."
  value       = <<-EOT

    1. Pull the producer connection string from Key Vault:
         az keyvault secret show \
           --vault-name ${azurerm_key_vault.main.name} \
           --name eventhubs-connection-string \
           --query value -o tsv

    2. Update repo .env:
         KAFKA_BOOTSTRAP_SERVERS=${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093
         EVENTHUBS_CONNECTION_STRING=<the value from step 1>

    3. Producer needs SASL_SSL config to talk to Event Hubs — see deploy/terraform/README.md
       for the snippet (not in src/ yet on this branch).

    4. Watch capture land in:
         abfss://capture@${azurerm_storage_account.lake.name}.dfs.core.windows.net/${azurerm_eventhub_namespace.main.name}/<topic>/...
  EOT
}
