#!/usr/bin/env bash
# Run the pipeline once against the local demo stack, then show what it did.
#
#   ./demo/run.sh
#
# Assumes `docker compose -f demo/docker-compose.yml up -d` and
# `./demo/setup.sh` have already run.
set -uo pipefail

cd "$(dirname "$0")/.."
PY=.venv/bin/python
DATE=2026-09-11

if [ ! -x "$PY" ]; then
  echo "No Python environment found. Run ./demo/setup.sh first -- it builds"
  echo "the virtual environment and prepares the demo data."
  exit 1
fi

# shellcheck disable=SC1091
source demo/env.sh

echo "============================================================"
echo " Running the pipeline for $DATE"
echo "============================================================"
echo

# A fresh run each time, so the demo is repeatable rather than accumulating
# state. Clear only the RUN HISTORY -- deleting airflow.db would also destroy
# the connections setup.sh registered, and every task would then fail with
# "Connection with ID aws_default not found".
rm -rf data
$PY - <<'PY'
import os, sqlite3
db = os.path.join(os.environ["AIRFLOW_HOME"], "airflow.db")
if os.path.exists(db):
    c = sqlite3.connect(db)
    # xcom first: it has a unique constraint keyed on the run, and orphaned
    # rows make the next run fail on push.
    for table in ("xcom", "task_instance", "dag_run"):
        try:
            c.execute(f"DELETE FROM {table}")
        except sqlite3.OperationalError:
            pass          # table absent on a first run
    c.commit()
PY
airflow db migrate >/dev/null 2>&1
airflow dags reserialize >/dev/null 2>&1

airflow dags test daily_insurance_claims_processing \
  --conf "{\"processing_date\": \"$DATE\"}" >/tmp/claims_demo_run.log 2>&1

echo "--- what each step did -------------------------------------"
sed 's/\x1b\[[0-9;]*m//g' /tmp/claims_demo_run.log \
  | grep -aoE "(insurance_claims_dag|kafka_publish|summary|gateways)\.py:[0-9]+\} (INFO|WARNING) - .*" \
  | sed -E 's/^[a-z_]+\.py:[0-9]+\} (INFO|WARNING) - /  /' \
  | grep -vE "^\s*$" | head -20

echo
echo "--- task outcomes ------------------------------------------"
$PY - <<'PY'
import sqlite3, os
db = os.path.join(os.environ["AIRFLOW_HOME"], "airflow.db")
c = sqlite3.connect(db)
order = ["check_input_file", "validate_claims", "enrich_claims",
         "calculate_risk_scores", "publish_to_kafka", "store_rejected_claims",
         "reconcile_run", "update_daily_summary", "archive_input_file",
         "send_notification"]
states = dict(c.execute("SELECT task_id, state FROM task_instance"))
for t in order:
    s = states.get(t) or "not reached"
    mark = {"success": "OK  ", "failed": "FAIL", "up_for_retry": "RETRY"}.get(s, "--  ")
    print(f"  {mark} {t:24s} {s}")
run = list(c.execute("SELECT state FROM dag_run"))
print(f"\n  DAG RUN: {run[0][0] if run else 'unknown'}")
PY

echo
echo "--- claims published to Kafka ------------------------------"
docker exec claimsdemo-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:19092 --topic insurance.claims.processed \
  --from-beginning --timeout-ms 8000 2>/dev/null \
  | $PY -c "
import sys, json
rows = [json.loads(l) for l in sys.stdin if l.strip()]
if not rows:
    print('  (none)')
else:
    # The topic keeps every run's events, which is the point of a log. Show
    # only the newest run so the demo output matches the run just made.
    latest = max(r['dag_run_id'] for r in rows)
    current = [r for r in rows if r['dag_run_id'] == latest]
    print(f\"  {'claim_id':22s} {'amount':>10s} {'risk':>5s}  status\")
    for r in current:
        print(f\"  {r['claim_id']:22s} {r['claim_amount']:>10s} {r['risk_score']:>5}  {r['processing_status']}\")
    print(f'\n  {len(current)} message(s) from this run'
          f' ({len(rows)} total on the topic across all runs)')
"

echo
echo "--- rejected claims in the S3 error folder -----------------"
$PY - <<'PY'
import os, boto3
s3 = boto3.client("s3", endpoint_url=os.environ["CLAIMS_S3_ENDPOINT"],
                  aws_access_key_id="claimsdemo", aws_secret_access_key="claimsdemo123",
                  region_name="us-east-1")
objs = s3.list_objects_v2(Bucket=os.environ["CLAIMS_S3_BUCKET"], Prefix="errors/").get("Contents", [])
if not objs:
    print("  (none - every claim was valid)")
for o in objs:
    print(f"  s3://{os.environ['CLAIMS_S3_BUCKET']}/{o['Key']}")
    body = s3.get_object(Bucket=os.environ["CLAIMS_S3_BUCKET"], Key=o["Key"])["Body"].read().decode()
    for line in body.strip().splitlines():
        print("    ", line[:150])
PY

echo
echo "--- the daily summary table --------------------------------"
docker exec claimsdemo-postgres psql -U claims -d claims -tAc \
  "SELECT 'date=' || processing_date || '  input=' || input_records ||
          '  successful=' || successful_claims || '  rejected=' || rejected_claims ||
          '  kafka_acked=' || kafka_acknowledged
     FROM daily_claims_summary ORDER BY processing_date;" 2>/dev/null \
  | sed 's/^/  /' | grep -v '^\s*$' || echo "  (table not written - the run did not reach that step)"

echo
echo "--- was the input file archived? ---------------------------"
$PY - <<'PY'
import os, boto3
s3 = boto3.client("s3", endpoint_url=os.environ["CLAIMS_S3_ENDPOINT"],
                  aws_access_key_id="claimsdemo", aws_secret_access_key="claimsdemo123",
                  region_name="us-east-1")
b = os.environ["CLAIMS_S3_BUCKET"]
for prefix, label in [("inbound/", "still in inbound"), ("archive/", "archived")]:
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=b, Prefix=prefix).get("Contents", [])]
    print(f"  {label:18s} {keys if keys else '(empty)'}")
PY

echo
echo "Full log: /tmp/claims_demo_run.log"
