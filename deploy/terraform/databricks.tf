# Databricks workspace-level resources.
#
# TWO-PASS APPLY:
#   Pass 1:  terraform apply -target=azurerm_databricks_workspace.main
#            Workspace is created; workspace_url becomes available.
#   Pass 2:  terraform apply
#            Secret scope, clusters, and jobs are configured.
#
# The databricks provider authenticates via ambient Azure CLI credentials —
# same `az login` session used by azurerm. The workspace creator is
# automatically a workspace admin, so no PAT is needed for Terraform.

locals {
  storage_account = azurerm_storage_account.lake.name

  abfss = {
    bronze      = "abfss://bronze@${local.storage_account}.dfs.core.windows.net"
    checkpoints = "abfss://checkpoints@${local.storage_account}.dfs.core.windows.net"
  }

  # Spark OAuth config for ADLS Gen2 — values sourced directly from Terraform
  # resources (already in state) to avoid {{secrets/...}} resolution at cluster
  # startup, which requires Databricks control-plane KV access that is unreliable.
  adls_spark_conf = {
    "spark.hadoop.fs.azure.account.auth.type.${local.storage_account}.dfs.core.windows.net"              = "OAuth"
    "spark.hadoop.fs.azure.account.oauth.provider.type.${local.storage_account}.dfs.core.windows.net"    = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    "spark.hadoop.fs.azure.account.oauth2.client.id.${local.storage_account}.dfs.core.windows.net"       = azuread_application.databricks.client_id
    "spark.hadoop.fs.azure.account.oauth2.client.secret.${local.storage_account}.dfs.core.windows.net"   = azuread_service_principal_password.databricks.value
    "spark.hadoop.fs.azure.account.oauth2.client.endpoint.${local.storage_account}.dfs.core.windows.net" = "https://login.microsoftonline.com/${data.azurerm_client_config.current.tenant_id}/oauth2/token"
  }

  # Env vars injected into every streaming job via cluster config.
  streaming_env = {
    KAFKA_BOOTSTRAP_SERVERS     = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
    EVENTHUBS_CONNECTION_STRING = azurerm_eventhub_namespace_authorization_rule.producer.primary_connection_string
    OUTPUT_FORMAT               = "delta"
    OUTPUT_PATH                 = local.abfss.bronze
    CHECKPOINT_PATH             = local.abfss.checkpoints
    KAFKA_STARTING_OFFSETS      = "earliest"
  }

  # Kafka + Avro Maven coordinates for DBR 15.x LTS (Scala 2.12, Spark 3.5).
  # Installed at cluster level — not via spark.jars.packages in app code.
  kafka_libs = [
    { maven = { coordinates = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" } },
    { maven = { coordinates = "org.apache.spark:spark-avro_2.12:3.5.0" } },
  ]

  # Shared single-node cluster config for all streaming jobs.
  streaming_cluster_base = {
    spark_version = data.databricks_spark_version.lts.id
    node_type_id  = data.databricks_node_type.d2s.id
    num_workers   = 0

    spark_conf = merge(local.adls_spark_conf, {
      "spark.master"                     = "local[*]"
      "spark.databricks.cluster.profile" = "singleNode"
    })
  }
}

# --- Cluster data sources ---

data "databricks_spark_version" "lts" {
  long_term_support = true
  scala             = "2.12"
}

data "databricks_node_type" "d2s" {
  min_memory_gb = 7   # Standard_D2s_v3 (2 core / 8 GB) — producer, Q1, Q3, dbt
}

data "databricks_node_type" "d4s" {
  min_memory_gb = 14  # Standard_D4s_v3 (4 core / 16 GB) — bronze, Q2
}

# --- Secret Scope (backed by Key Vault) ---
# All secrets added to KV (by Terraform or manually) are surfaced under
# the scope name "at-pipeline". Jobs reference them as:
#   {{secrets/at-pipeline/<secret-name>}}

resource "databricks_secret_scope" "at_pipeline" {
  name = "at-pipeline"

  keyvault_metadata {
    resource_id = azurerm_key_vault.main.id
    dns_name    = azurerm_key_vault.main.vault_uri
  }

  depends_on = [azurerm_databricks_workspace.main]
}

resource "databricks_secret_acl" "at_pipeline_users_read" {
  scope      = databricks_secret_scope.at_pipeline.name
  principal  = "users"
  permission = "READ"
}

# --- Git credential (needed if repo is private) ---
# Remove this block if the GitHub repo is public.

resource "databricks_git_credential" "github" {
  count                 = var.github_pat != "" ? 1 : 0
  git_provider          = "gitHub"
  git_username          = var.github_username
  personal_access_token = var.github_pat
}

# --- Repo checkout ---

resource "databricks_repo" "pipeline" {
  url    = "https://github.com/${var.github_username}/at-streaming-data-pipeline"
  branch = "main"
  path   = "/Repos/${var.github_username}/at-streaming-data-pipeline"

  depends_on = [databricks_git_credential.github]
}

locals {
  repo_path = databricks_repo.pipeline.path
}

# --- Job: Streaming Pipeline (all queries on one cluster) ---
# Bronze ingestion + Q1 delay alerts + Q2 vehicle stalls + Q3 headway regularity
# run as concurrent Structured Streaming queries in a single process.
# Previously 4 jobs × mixed D2s/D4s clusters; now one D4s 24/7 — ~57% cheaper.

resource "databricks_job" "streaming" {
  name = "at-streaming-pipeline"

  task {
    task_key = "stream"

    new_cluster {
      spark_version  = local.streaming_cluster_base.spark_version
      node_type_id   = data.databricks_node_type.d4s.id
      num_workers    = local.streaming_cluster_base.num_workers
      spark_conf     = local.streaming_cluster_base.spark_conf
      spark_env_vars = local.streaming_env

      azure_attributes {
        availability       = "SPOT_WITH_FALLBACK_AZURE"
        spot_bid_max_price = -1
      }
    }

    # provider 1.50+: libraries must be at task level, not inside new_cluster
    dynamic "library" {
      for_each = local.kafka_libs
      content {
        maven { coordinates = library.value.maven.coordinates }
      }
    }

    library {
      pypi { package = "python-dotenv>=1.0" }
    }

    spark_python_task {
      python_file = "${local.repo_path}/src/streaming/main.py"
    }
  }

  continuous { pause_status = "UNPAUSED" }
  depends_on  = [databricks_secret_scope.at_pipeline, databricks_repo.pipeline]
}

# --- Job: dbt (hourly scheduled) ---
# PAUSED by default — unpause after verifying Bronze Delta tables exist.

resource "databricks_job" "dbt" {
  name = "dbt-transform"

  task {
    task_key = "transform"

    new_cluster {
      spark_version = data.databricks_spark_version.lts.id
      node_type_id  = data.databricks_node_type.d2s.id
      num_workers   = 0

      azure_attributes {
        availability       = "SPOT_WITH_FALLBACK_AZURE"
        spot_bid_max_price = -1
      }

      spark_conf = {
        "spark.master"                     = "local[*]"
        "spark.databricks.cluster.profile" = "singleNode"
      }

      spark_env_vars = {
        DATABRICKS_HOST      = "https://${azurerm_databricks_workspace.main.workspace_url}"
        DATABRICKS_TOKEN     = "{{secrets/at-pipeline/databricks-pat-token}}"
        DATABRICKS_HTTP_PATH = "{{secrets/at-pipeline/databricks-http-path}}"
      }
    }

    spark_python_task {
      python_file = "${local.repo_path}/deploy/databricks/run_dbt.py"
    }
  }

  schedule {
    quartz_cron_expression = "0 0 * ? * *"
    timezone_id            = "Pacific/Auckland"
    pause_status           = "PAUSED"
  }

  depends_on = [databricks_secret_scope.at_pipeline, databricks_repo.pipeline]
}

