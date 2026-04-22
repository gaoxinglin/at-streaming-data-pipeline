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
from pathlib import Path

subprocess.check_call([sys.executable, "-m", "pip", "install", "dbt-databricks>=1.8,<2", "-q"])

import shutil
dbt_bin = shutil.which("dbt") or f"{sys.executable.rsplit('/', 1)[0]}/dbt"

repo_root = Path(sys.argv[0]).resolve().parents[2]
project_dir = repo_root / "transform"

dbt_args = ["--target", "prod", "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)]

subprocess.check_call([dbt_bin, "deps"] + dbt_args)

result = subprocess.run([dbt_bin, "run"] + dbt_args, capture_output=False)
sys.exit(result.returncode)
