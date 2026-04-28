"""
dbt runner for Databricks Jobs.

Invoked as a spark_python_task. Runs `dbt run --target prod` from the repo
root. Env vars (DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH) are
injected via the job's cluster spark_env_vars — see deploy/terraform/databricks.tf.

Set DBT_FULL_REFRESH=1 in the job's spark_env_vars for a one-time full rebuild,
then remove it to resume incremental runs.

dbt-databricks is not bundled with Databricks Runtime, so we install it at
runtime via pip. The install is fast (~10s) because DBR already has most deps.
"""
import os
import subprocess
import sys
from pathlib import Path

subprocess.check_call(
    [sys.executable, "-m", "pip", "install", "dbt-databricks>=1.8,<2", "-q", "-q"],
    stderr=subprocess.DEVNULL,
)

import shutil
dbt_bin = shutil.which("dbt") or f"{sys.executable.rsplit('/', 1)[0]}/dbt"

repo_root = Path(sys.argv[0]).resolve().parents[2]
project_dir = repo_root / "transform"

dbt_args = ["--target", "prod", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)]
full_refresh = ["--full-refresh"] if os.getenv("DBT_FULL_REFRESH") else []

subprocess.check_call([dbt_bin, "deps"] + dbt_args)
subprocess.check_call([dbt_bin, "seed"] + dbt_args)
subprocess.check_call([dbt_bin, "run"] + full_refresh + dbt_args)
subprocess.check_call([dbt_bin, "test"] + dbt_args)
