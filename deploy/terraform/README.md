# Azure deployment

Provisions the full AT streaming data pipeline stack on Azure.

## What this creates

**`rg-atpipe-dev`**
- ADLS Gen2 storage account — containers: `capture`, `bronze`, `silver`, `gold`, `checkpoints`
- Event Hubs namespace (Standard, 1 TU, Kafka-enabled) + topics: `vehicle_positions`, `trip_updates`, `service_alerts`, `alerts`, `headway_metrics`
- Azure Container Registry (Basic) — hosts the `at-producer` Docker image
- ACI producer — 0.25 vCPU container that polls AT API and publishes to Event Hubs
- Key Vault — Event Hubs connection string, AT API key, Databricks SP credentials
- Databricks workspace (Premium)
- Monthly budget alert ($300 USD default) at 80% forecast and 100% actual

**`rg-atpipe-dev-managed`** (auto-managed by Databricks)
- VNet with public/private subnets — VNet injection means no NAT gateway, saving ~$32/month vs default Databricks networking

## Prerequisites

```bash
brew install azure-cli terraform
az login
az account show   # confirm the right subscription is active
```

## Apply (two-pass required)

Databricks workspace-level resources (clusters, jobs, secret scope) need the workspace URL to exist first.

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # set owner, at_api_key, github_pat
terraform init

# Pass 1: create workspace + networking (~10 min)
terraform apply -target=azurerm_databricks_workspace.main \
                -target=azurerm_virtual_network.databricks \
                -target=azurerm_network_security_group.databricks \
                -target=azurerm_subnet.databricks_public \
                -target=azurerm_subnet.databricks_private \
                -target=azurerm_subnet_network_security_group_association.databricks_public \
                -target=azurerm_subnet_network_security_group_association.databricks_private

# Pass 2: everything else (~5 min)
terraform apply
```

## Push the producer image

ACI needs the image in ACR before it can start. Run once after ACR is created:

```bash
ACR=$(terraform output -raw acr_login_server | cut -d. -f1)
az acr build --registry "$ACR" --image at-producer:latest \
  -f src/producer/Dockerfile src/producer/
```

## Estimated cost (dev, idle)

| Resource | ~AUD/month |
|----------|-----------|
| Databricks workspace (no running cluster) | ~$5 |
| Event Hubs (1 TU) | ~$13 |
| ACR Basic | ~$6 |
| ACI producer (0.25 vCPU, always-on) | ~$3 |
| Storage + Key Vault | ~$2 |
| **Total idle** | **~$29** |

Databricks cluster compute (D4s_v3 spot) adds ~$2–4/hour when running.

## Tear down / restore cycle

To pause and save costs, delete from the Azure portal:
- `aci-atpipe-dev-producer` (Container instances)
- `evhns-atpipe-dev-*` (Event Hubs Namespace)
- `acratpipedev*` (Container registry)
- Databricks workspace (triggers auto-cleanup of managed RG including networking)

To restore, run the two-pass apply above.
