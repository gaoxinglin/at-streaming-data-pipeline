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

  # Spark OAuth config for ADLS Gen2 — applied at cluster level so job code
  # stays auth-agnostic. Credentials injected at runtime via secret scope.
  adls_spark_conf = {
    "spark.hadoop.fs.azure.account.auth.type.${local.storage_account}.dfs.core.windows.net"              = "OAuth"
    "spark.hadoop.fs.azure.account.oauth.provider.type.${local.storage_account}.dfs.core.windows.net"    = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    "spark.hadoop.fs.azure.account.oauth2.client.id.${local.storage_account}.dfs.core.windows.net"       = "{{secrets/at-pipeline/databricks-sp-client-id}}"
    "spark.hadoop.fs.azure.account.oauth2.client.secret.${local.storage_account}.dfs.core.windows.net"   = "{{secrets/at-pipeline/databricks-sp-client-secret}}"
    "spark.hadoop.fs.azure.account.oauth2.client.endpoint.${local.storage_account}.dfs.core.windows.net" = "https://login.microsoftonline.com/{{secrets/at-pipeline/databricks-sp-tenant-id}}/oauth2/token"
  }

  # Env vars injected into every streaming job via cluster config.
  streaming_env = {
    KAFKA_BOOTSTRAP_SERVERS     = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
    EVENTHUBS_CONNECTION_STRING = "{{secrets/at-pipeline/eventhubs-connection-string}}"
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
  # when the job finishes (or run indefinitely for continuous jobs).
  streaming_cluster_base = {
    spark_version = data.databricks_spark_version.lts.id
    node_type_id  = data.databricks_node_type.d2s.id  # default small; override to d4s where needed
    num_workers   = 0  # single-node: driver runs executors too

    spark_conf = merge(local.adls_spark_conf, {
      "spark.master"                     = "local[*]"
      "spark.databricks.cluster.profile" = "singleNode"
    })
  }
}

# --- Cluster data sources ---

data "databricks_spark_version" "lts" {
  long_term_support = true
}

data "databricks_node_type" "d2s" {
  min_memory_gb = 7   # Standard_D2s_v3 (2 core / 8 GB) — producer, Q1, Q3, dbt
}

data "databricks_node_type" "d4s" {
  min_memory_gb = 14  # Standard_D4s_v3 (4 core / 16 GB) — bronze, Q2, Q4
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

# --- Git credential (needed if repo is private) ---
# Remove this block if the GitHub repo is public.

resource "databricks_git_credential" "github" {
  git_provider          = "gitHub"
  git_username          = var.github_username
  personal_access_token = var.github_pat
}

# --- Repo checkout ---
# Databricks checks out the repo into /Repos/<owner>/<repo>.
# spark_python_task paths are relative to this checkout root.

resource "databricks_repo" "pipeline" {
  url    = "https://github.com/${var.github_username}/at-streaming-data-pipeline"
  branch = "main"
  path   = "/Repos/${var.github_username}/at-streaming-data-pipeline"

  depends_on = [databricks_git_credential.github]
}

locals {
  repo_path = databricks_repo.pipeline.path
}

# --- Job: AT Producer ---
# Continuous — Databricks restarts immediately on exit or failure.

resource "databricks_job" "producer" {
  name = "at-producer"

  git_source {
    url      = "https://github.com/${var.github_username}/at-streaming-data-pipeline"
    branch   = "main"
    provider = "gitHub"
  }

  task {
    task_key = "produce"

    new_cluster {
      spark_version = data.databricks_spark_version.lts.id
      node_type_id  = data.databricks_node_type.d2s.id  # pure Python, no Spark needed
      num_workers   = 0

      spark_conf = {
        "spark.master"                     = "local[*]"
        "spark.databricks.cluster.profile" = "singleNode"
      }

      spark_env_vars = {
        AT_API_KEY                  = "{{secrets/at-pipeline/at-api-key}}"
        EVENTHUBS_CONNECTION_STRING = "{{secrets/at-pipeline/eventhubs-connection-string}}"
        KAFKA_BOOTSTRAP_SERVERS     = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
      }
    }

    spark_python_task {
      python_file = "src/ingestion/at_producer.py"
    }
  }

  continuous { pause_status = "UNPAUSED" }

  depends_on = [databricks_secret_scope.at_pipeline, databricks_repo.pipeline]
}

# --- Job: Bronze Ingestion ---

resource "databricks_job" "bronze_ingestion" {
  name = "bronze-ingestion"

  git_source {
    url      = "https://github.com/${var.github_username}/at-streaming-data-pipeline"
    branch   = "main"
    provider = "gitHub"
  }

  task {
    task_key = "ingest"

    new_cluster {
      spark_version  = local.streaming_cluster_base.spark_version
      node_type_id   = data.databricks_node_type.d4s.id  # 3 parallel streams need more memory
      num_workers    = local.streaming_cluster_base.num_workers
      spark_conf     = local.streaming_cluster_base.spark_conf
      spark_env_vars = local.streaming_env

      dynamic "library" {
        for_each = local.kafka_libs
        content {
          maven { coordinates = library.value.maven.coordinates }
        }
      }
    }

    spark_python_task {
      python_file = "src/streaming/bronze_ingestion.py"
    }
  }

  continuous { pause_status = "UNPAUSED" }
  depends_on  = [databricks_secret_scope.at_pipeline, databricks_repo.pipeline]
}

# --- Job: Q1 Delay Alert ---

resource "databricks_job" "delay_alert" {
  name = "q1-delay-alert"

  git_source {
    url      = "https://github.com/${var.github_username}/at-streaming-data-pipeline"
    branch   = "main"
    provider = "gitHub"
  }

  task {
    task_key = "detect"

    new_cluster {
      spark_version           = local.streaming_cluster_base.spark_version
      node_type_id            = local.streaming_cluster_base.node_type_id
      num_workers             = local.streaming_cluster_base.num_workers
      spark_conf              = local.streaming_cluster_base.spark_conf
      spark_env_vars          = local.streaming_env

      dynamic "library" {
        for_each = local.kafka_libs
        content {
          maven { coordinates = library.value.maven.coordinates }
        }
      }
    }

    spark_python_task {
      python_file = "src/streaming/delay_alert_job.py"
    }
  }

  continuous { pause_status = "UNPAUSED" }
  depends_on  = [databricks_secret_scope.at_pipeline, databricks_repo.pipeline]
}

# --- Job: Q2 Vehicle Stall ---

resource "databricks_job" "vehicle_stall" {
  name = "q2-vehicle-stall"

  git_source {
    url      = "https://github.com/${var.github_username}/at-streaming-data-pipeline"
    branch   = "main"
    provider = "gitHub"
  }

  task {
    task_key = "detect"

    new_cluster {
      spark_version  = local.streaming_cluster_base.spark_version
      node_type_id   = data.databricks_node_type.d4s.id  # applyInPandasWithState needs state memory
      num_workers    = local.streaming_cluster_base.num_workers
      spark_conf     = local.streaming_cluster_base.spark_conf
      spark_env_vars = local.streaming_env

      dynamic "library" {
        for_each = local.kafka_libs
        content {
          maven { coordinates = library.value.maven.coordinates }
        }
      }
    }

    spark_python_task {
      python_file = "src/streaming/vehicle_stall_job.py"
    }
  }

  continuous { pause_status = "UNPAUSED" }
  depends_on  = [databricks_secret_scope.at_pipeline, databricks_repo.pipeline]
}

# --- Job: Q3 Headway Regularity ---

resource "databricks_job" "headway_regularity" {
  name = "q3-headway-regularity"

  git_source {
    url      = "https://github.com/${var.github_username}/at-streaming-data-pipeline"
    branch   = "main"
    provider = "gitHub"
  }

  task {
    task_key = "detect"

    new_cluster {
      spark_version           = local.streaming_cluster_base.spark_version
      node_type_id            = local.streaming_cluster_base.node_type_id
      num_workers             = local.streaming_cluster_base.num_workers
      spark_conf              = local.streaming_cluster_base.spark_conf
      spark_env_vars          = local.streaming_env

      dynamic "library" {
        for_each = local.kafka_libs
        content {
          maven { coordinates = library.value.maven.coordinates }
        }
      }
    }

    spark_python_task {
      python_file = "src/streaming/headway_regularity_job.py"
    }
  }

  continuous { pause_status = "UNPAUSED" }
  depends_on  = [databricks_secret_scope.at_pipeline, databricks_repo.pipeline]
}

# --- Job: Q4 Alert Correlation ---

resource "databricks_job" "alert_correlation" {
  name = "q4-alert-correlation"

  git_source {
    url      = "https://github.com/${var.github_username}/at-streaming-data-pipeline"
    branch   = "main"
    provider = "gitHub"
  }

  task {
    task_key = "detect"

    new_cluster {
      spark_version = local.streaming_cluster_base.spark_version
      node_type_id  = data.databricks_node_type.d4s.id  # 3-stream join + window state
      num_workers   = local.streaming_cluster_base.num_workers

      spark_conf = merge(local.streaming_cluster_base.spark_conf, {
        "spark.driver.memory" = "2g"
      })

      spark_env_vars = local.streaming_env

      dynamic "library" {
        for_each = local.kafka_libs
        content {
          maven { coordinates = library.value.maven.coordinates }
        }
      }
    }

    spark_python_task {
      python_file = "src/streaming/alert_correlation_job.py"
    }
  }

  continuous { pause_status = "UNPAUSED" }
  depends_on  = [databricks_secret_scope.at_pipeline, databricks_repo.pipeline]
}

# --- Job: dbt (hourly scheduled) ---
# Runs dbt against Databricks SQL Warehouse via a thin Python wrapper notebook.
# PAUSED by default — unpause after verifying Bronze Delta tables exist.

resource "databricks_job" "dbt" {
  name = "dbt-transform"

  git_source {
    url      = "https://github.com/${var.github_username}/at-streaming-data-pipeline"
    branch   = "main"
    provider = "gitHub"
  }

  task {
    task_key = "transform"

    new_cluster {
      spark_version = data.databricks_spark_version.lts.id
      node_type_id  = data.databricks_node_type.d2s.id  # dbt just sends SQL, no Spark compute
      num_workers   = 0

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
      python_file = "deploy/databricks/run_dbt.py"
    }
  }

  schedule {
    quartz_cron_expression = "0 0 * ? * *"  # top of every hour, Auckland time
    timezone_id            = "Pacific/Auckland"
    pause_status           = "PAUSED"
  }

  depends_on = [databricks_secret_scope.at_pipeline, databricks_repo.pipeline]
}
