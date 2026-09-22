#!/usr/bin/env bash
# Prepare the demo environment: buckets, Airflow connections, and the day's
# claims file. Safe to re-run.
#
#   ./demo/setup.sh            the happy path (5 good claims)
#   ./demo/setup.sh rejects    2 of 6 claims will be rejected
#
# Run ./demo/env.sh first, or source it, so the pipeline points at the demo
# stack rather than at real systems.
set -euo pipefail

cd "$(dirname "$0")/.."
SCENARIO="${1:-happy}"
PY=.venv/bin/python

echo "=== 0. Python environment ==="
# Build the virtual environment if it is not already there, so this script
# works on a freshly cloned repository with no prior setup.
if [ ! -x "$PY" ]; then
  PY312="$(command -v python3.12 || true)"
  if [ -z "$PY312" ]; then
    echo "  Python 3.12 is required and was not found on PATH."
    echo "  Airflow 3.0.2 does not support Python 3.13 yet."
    echo "    macOS:  brew install python@3.12"
    echo "    Ubuntu: sudo apt install python3.12 python3.12-venv"
    exit 1
  fi
  echo "  creating .venv with $("$PY312" --version)"
  "$PY312" -m venv .venv
  echo "  installing dependencies (a few minutes the first time)..."
  .venv/bin/pip install -q --upgrade pip
  .venv/bin/pip install -q -r requirements.txt
  echo "  done"
else
  echo "  .venv already present"
fi

# shellcheck disable=SC1091
source demo/env.sh

echo
echo "=== 1. Waiting for the demo stack ==="
for svc in claimsdemo-minio claimsdemo-postgres claimsdemo-kafka claimsdemo-policy-api; do
  printf "  %-28s" "$svc"
  for _ in $(seq 1 60); do
    status=$(docker inspect -f '{{.State.Health.Status}}' "$svc" 2>/dev/null || echo "missing")
    [ "$status" = "healthy" ] && break
    sleep 2
  done
  echo "$status"
  [ "$status" = "healthy" ] || { echo "  -> $svc never became healthy. docker compose -f demo/docker-compose.yml logs $svc"; exit 1; }
done

echo
echo "=== 2. Creating the S3 bucket ==="
$PY - <<'EOF'
import os, boto3
s3 = boto3.client("s3", endpoint_url=os.environ["CLAIMS_S3_ENDPOINT"],
                  aws_access_key_id="claimsdemo",
                  aws_secret_access_key="claimsdemo123",
                  region_name="us-east-1")
bucket = os.environ["CLAIMS_S3_BUCKET"]
existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
if bucket not in existing:
    s3.create_bucket(Bucket=bucket)
    print(f"  created s3://{bucket}")
else:
    print(f"  s3://{bucket} already exists")
EOF

echo
echo "=== 3. Preparing Airflow ==="
# Airflow keeps its own small bookkeeping database. For the demo this is a
# local SQLite file under demo/airflow/, entirely separate from anything else
# on this machine.
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
  airflow db migrate 2>&1 | tail -1 | sed 's/^/  /'
else
  echo "  metadata database already initialised"
fi
airflow dags reserialize 2>&1 | grep -c "Creating ORM DAG\|Sync" >/dev/null || true
echo "  DAG registered: $(airflow dags list 2>/dev/null | grep -c daily_insurance_claims_processing) found"

echo
echo "=== 4. Registering Airflow connections ==="
# These are what the pipeline resolves credentials through -- the same
# mechanism the client will use for their real systems.
airflow connections delete aws_default            >/dev/null 2>&1 || true
airflow connections delete claims_postgres        >/dev/null 2>&1 || true
airflow connections delete claims_policy_api      >/dev/null 2>&1 || true
airflow connections delete claims_kafka           >/dev/null 2>&1 || true

airflow connections add aws_default \
  --conn-type aws \
  --conn-login claimsdemo \
  --conn-password claimsdemo123 \
  --conn-extra "{\"endpoint_url\": \"${CLAIMS_S3_ENDPOINT}\", \"region_name\": \"us-east-1\"}" >/dev/null
echo "  aws_default          -> MinIO (S3)"

airflow connections add claims_postgres \
  --conn-type postgres --conn-host localhost --conn-port 15432 \
  --conn-login claims --conn-password claims --conn-schema claims >/dev/null
echo "  claims_postgres      -> PostgreSQL"

airflow connections add claims_policy_api \
  --conn-type http --conn-host "http://localhost:18000" \
  --conn-password "demo-api-token" >/dev/null
echo "  claims_policy_api    -> policy API (bearer token)"

airflow connections add claims_kafka \
  --conn-type generic --conn-host localhost >/dev/null
echo "  claims_kafka         -> Kafka (plaintext for the demo)"

echo
echo "=== 5. Uploading today's claims file ==="
$PY - "$SCENARIO" <<'EOF'
import os, sys, boto3
scenario = sys.argv[1]
fixture = {"happy":   "qa/fixtures/claims_20260911_happy.csv",
           "rejects": "qa/fixtures/claims_20260911_mixed_rejections.csv"}.get(scenario)
if fixture is None:
    sys.exit(f"unknown scenario {scenario!r}; use 'happy' or 'rejects'")

s3 = boto3.client("s3", endpoint_url=os.environ["CLAIMS_S3_ENDPOINT"],
                  aws_access_key_id="claimsdemo",
                  aws_secret_access_key="claimsdemo123",
                  region_name="us-east-1")
bucket, key = os.environ["CLAIMS_S3_BUCKET"], "inbound/claims_20260911.csv"

# Clear the previous run's outputs, so what you see after ./demo/run.sh is
# this run's work and not a mix of several. The pipeline itself overwrites
# per-date keys on a re-run; this is only to keep the demo legible when
# switching between the happy and rejects scenarios.
for prefix in ("errors/", "archive/"):
    old = s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    for obj in old:
        s3.delete_object(Bucket=bucket, Key=obj["Key"])
    if old:
        print(f"  cleared {len(old)} object(s) under {prefix}")

body = open(fixture, "rb").read()
s3.put_object(Bucket=bucket, Key=key, Body=body)
rows = len(body.decode().strip().splitlines()) - 1
print(f"  {fixture}")
print(f"  -> s3://{bucket}/{key}  ({rows} claims, scenario: {scenario})")
EOF

echo
echo "Ready. Now run:  ./demo/run.sh"
