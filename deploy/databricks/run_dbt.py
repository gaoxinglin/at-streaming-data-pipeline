"""
dbt runner for Databricks Jobs.

Invoked as a spark_python_task. Runs `dbt run --target prod` from the repo
root. Env vars (DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH) are
injected via the job's cluster spark_env_vars — see deploy/terraform/databricks.tf.

dbt-databricks is not bundled with Databricks Runtime, so we install it at
runtime via pip. The install is fast (~10s) because DBR already has most deps.
"""
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip", "install", "dbt-databricks>=1.8", "-q"])

result = subprocess.run(
    [sys.executable, "-m", "dbt", "run", "--target", "prod", "--project-dir", "transform"],
    capture_output=False,
)

sys.exit(result.returncode)
