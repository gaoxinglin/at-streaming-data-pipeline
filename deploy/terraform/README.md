# Azure deployment — Phase 1 (collect-first stack)

Provisions just enough Azure to start collecting AT real-time data into the cloud
cheaply. **No Databricks yet** — we let Event Hubs Capture land everything in ADLS
as Avro for as long as you want, then add compute later in Phase 2.

## What this creates

- Resource group `rg-atpipe-dev`
- ADLS Gen2 storage account with containers: `capture`, `bronze`, `silver`, `gold`, `checkpoints`
- Event Hubs namespace (Standard, 1 TU, Kafka-enabled) + topics: `vehicle_positions`, `trip_updates`, `service_alerts`, `at_alerts`
- **Event Hubs Capture** writing every 5 min from each topic into `capture/` as Avro
- Key Vault with the producer's connection string (and optionally your AT API key)

Steady-state cost is roughly **$1/day** — most of it the 1 throughput unit on EH.

## Prerequisites

```bash
brew install azure-cli terraform
az login
az account show   # confirm the right subscription is default
```

## Apply

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars     # at minimum: set `owner`

terraform init
terraform plan
terraform apply
```

Apply takes ~3–5 min. Resource names get a 5-char random suffix because storage
account / Key Vault / EH namespace names must be globally unique.

## Wire the local producer to Azure

After apply:

```bash
KV=$(terraform output -raw key_vault_name)
EH_BOOTSTRAP=$(terraform output -raw eventhubs_kafka_bootstrap)
EH_CONN=$(az keyvault secret show --vault-name "$KV" --name eventhubs-connection-string --query value -o tsv)

# update the repo .env
echo "KAFKA_BOOTSTRAP_SERVERS=$EH_BOOTSTRAP" >> ../../.env
echo "EVENTHUBS_CONNECTION_STRING=$EH_CONN" >> ../../.env
```

The producer also needs SASL_SSL config to actually talk to Event Hubs.
That code change is **not in this branch** — it belongs in a follow-up. The
gist of it (for `confluent-kafka-python`):

```python
conf = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": os.environ["EVENTHUBS_CONNECTION_STRING"],
}
```

Gate it on `EVENTHUBS_CONNECTION_STRING` being set so local Redpanda dev still works.

## Verify capture is working

After ~5 min of producer running:

```bash
SA=$(terraform output -raw storage_account_name)
az storage blob list \
  --account-name "$SA" \
  --container-name capture \
  --auth-mode login \
  --query "[].name" -o tsv | head
```

You should see files under `<namespace>/<topic>/<partition>/<yyyy>/<mm>/<dd>/<hh>/<mm>/<ss>.avro`.

If not after 10 min: check the namespace's Capture diagnostics in the portal —
9 times out of 10 it's the role assignment to the storage account not having
propagated. Wait 5 min and try again, or `terraform apply` once more.

## Tear down

```bash
terraform destroy
```

Soft-delete is on for storage and Key Vault (7 days). If you want the names
back immediately, purge them from the portal — otherwise just pick fresh names
on the next deploy (the random suffix already handles that).
