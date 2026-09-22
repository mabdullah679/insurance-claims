# Point the pipeline at the local demo stack instead of real systems.
#
#   source demo/env.sh
#
# Every one of these is a normal configuration value documented in the README.
# Nothing here is demo-only machinery: pointing at the client's real systems
# means changing these same values (and the Airflow connections), not the code.

# Resolve this file's own directory under bash OR zsh. `BASH_SOURCE` is unset
# in zsh, where `${(%):-%x}` is the equivalent; without this the paths below
# silently resolve against the caller's cwd.
if [ -n "${BASH_SOURCE:-}" ]; then
  _demo_src="${BASH_SOURCE[0]}"
elif [ -n "${ZSH_VERSION:-}" ]; then
  _demo_src="${(%):-%x}"
else
  _demo_src="demo/env.sh"
fi
_DEMO_DIR="$(cd "$(dirname "$_demo_src")" && pwd)"
_PROJECT_DIR="$(cd "$_DEMO_DIR/.." && pwd)"

# Where Airflow keeps its own bookkeeping. Under demo/ so it is disposable and
# cannot disturb any other Airflow on this machine.
export AIRFLOW_HOME="$_DEMO_DIR/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$_PROJECT_DIR/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# The previous-day arithmetic assumes this is False (see FINAL-REPORT.md N1).
export AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS=False

# S3 -- MinIO standing in for the client's bucket
export CLAIMS_S3_BUCKET=claims-inbound
export CLAIMS_S3_ENDPOINT=http://localhost:19000
export CLAIMS_S3_CONN_ID=aws_default

# PostgreSQL -- customers, and the daily summary table
export CLAIMS_PG_CONN_ID=claims_postgres
export CLAIMS_CUSTOMER_TABLE=customers
export CLAIMS_SUMMARY_TABLE=daily_claims_summary

# The policy REST API
export CLAIMS_POLICY_API_CONN_ID=claims_policy_api
export CLAIMS_POLICY_API_BASE=http://localhost:18000

# Kafka
export KAFKA_BOOTSTRAP=localhost:19092
export KAFKA_CLAIMS_TOPIC=insurance.claims.processed
export KAFKA_CLAIMS_DLQ_TOPIC=insurance.claims.dlq

# Operational dials from requirements section 5
export CLAIMS_REJECTION_RATE_THRESHOLD=0.10
export CLAIMS_SLA_MINUTES=90

# NOTE on timing. The DAG sets retries=2 with a 5-minute delay in its own
# default_args, which is correct for production and cannot be overridden by an
# environment variable. So if a task genuinely fails during a demo, the run
# will sit waiting out that delay rather than ending immediately. Ctrl-C is
# fine -- demo/run.sh reports the task states from the database either way.

# NOTE: CLAIMS_RISK_SCORER is deliberately NOT set. The pipeline therefore
# uses its documented placeholder scorer and says so loudly in the logs. That
# is the one outstanding item awaiting the client's scoring rules.

export PATH="$_PROJECT_DIR/.venv/bin:$PATH"
unset _demo_src _DEMO_DIR _PROJECT_DIR
