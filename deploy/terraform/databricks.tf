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

# --- Git credential (needed if repo is private) ---
# Remove this block if the GitHub repo is public.

resource "databricks_git_credential" "github" {
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
      node_type_id  = data.databricks_node_type.d2s.id
      num_workers   = 0

      # Spot with on-demand fallback — checkpoint + continuous restart means
      # an eviction causes at most one polling cycle of data loss (~30s).
      azure_attributes {
        availability       = "SPOT_WITH_FALLBACK_AZURE"
        spot_bid_max_price = -1
      }

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
      node_type_id   = data.databricks_node_type.d4s.id
      num_workers    = local.streaming_cluster_base.num_workers
      spark_conf     = local.streaming_cluster_base.spark_conf
      spark_env_vars = local.streaming_env

      azure_attributes {
        availability       = "SPOT_WITH_FALLBACK_AZURE"
        spot_bid_max_price = -1
      }

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

      azure_attributes {
        availability       = "SPOT_WITH_FALLBACK_AZURE"
        spot_bid_max_price = -1
      }

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
      node_type_id   = data.databricks_node_type.d4s.id
      num_workers    = local.streaming_cluster_base.num_workers
      spark_conf     = local.streaming_cluster_base.spark_conf
      spark_env_vars = local.streaming_env

      azure_attributes {
        availability       = "SPOT_WITH_FALLBACK_AZURE"
        spot_bid_max_price = -1
      }

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

      azure_attributes {
        availability       = "SPOT_WITH_FALLBACK_AZURE"
        spot_bid_max_price = -1
      }

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

# --- Job: dbt (hourly scheduled) ---
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
      python_file = "deploy/databricks/run_dbt.py"
    }
  }

  schedule {
    quartz_cron_expression = "0 0 * ? * *"
    timezone_id            = "Pacific/Auckland"
    pause_status           = "PAUSED"
  }

  depends_on = [databricks_secret_scope.at_pipeline, databricks_repo.pipeline]
}

# --- Night-mode scheduler: pause / resume Q1-Q3 detection jobs ---
#
# AT off-peak is 22:00-06:00 NZST (5-min polling, minimal event volume).
# Detection jobs (Q1-Q3) pause at 22:00 and resume at 06:00, saving ~8hr of
# cluster cost per day. Producer + bronze ingest stay up to keep the capture
# buffer warm and avoid checkpoint gaps.
#
# The notebook calls the Databricks Jobs API using the cluster's ambient token
# (dbutils.notebook.getContext().apiToken()) — no stored PAT needed.

resource "databricks_notebook" "toggle_detection" {
  path     = "/Shared/at-pipeline/toggle_detection_jobs"
  language = "PYTHON"

  content_base64 = base64encode(<<-EOF
    import requests

    dbutils.widgets.addText("action", "PAUSED")
    action = dbutils.widgets.get("action")

    ctx   = dbutils.notebook.getContext()
    token = ctx.apiToken().get()
    host  = ctx.apiUrl().get()

    target = {"q1-delay-alert", "q2-vehicle-stall", "q3-headway-regularity"}
    hdrs   = {"Authorization": f"Bearer {token}"}

    jobs = requests.get(f"{host}/api/2.1/jobs/list?limit=100", headers=hdrs).json().get("jobs", [])
    for job in jobs:
        if job["settings"]["name"] in target:
            requests.post(f"{host}/api/2.1/jobs/update", headers=hdrs, json={
                "job_id": job["job_id"],
                "new_settings": {"continuous": {"pause_status": action}},
            })
            print(f"[{action}] {job['settings']['name']} (id={job['job_id']})")
    EOF
  )
}

resource "databricks_job" "pause_detection" {
  name = "night-pause-detection"

  task {
    task_key = "pause"

    notebook_task {
      notebook_path   = databricks_notebook.toggle_detection.path
      base_parameters = { action = "PAUSED" }
    }

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
    }
  }

  # 22:00 NZST daily — AT off-peak begins
  schedule {
    quartz_cron_expression = "0 0 22 * * ?"
    timezone_id            = "Pacific/Auckland"
    pause_status           = "UNPAUSED"
  }

  depends_on = [databricks_notebook.toggle_detection]
}

resource "databricks_job" "resume_detection" {
  name = "morning-resume-detection"

  task {
    task_key = "resume"

    notebook_task {
      notebook_path   = databricks_notebook.toggle_detection.path
      base_parameters = { action = "UNPAUSED" }
    }

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
    }
  }

  # 06:00 NZST daily — AT peak begins
  schedule {
    quartz_cron_expression = "0 0 6 * * ?"
    timezone_id            = "Pacific/Auckland"
    pause_status           = "UNPAUSED"
  }

  depends_on = [databricks_notebook.toggle_detection]
}
