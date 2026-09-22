"""A stub standing in for the client's policy REST API.

Serves the five policies from the QA workbook's `Policies_QA` tab, in the
shape `claims/common/gateways.py::PolicyApiLookup` expects:

    GET /policies?policy_ids=POL-QA-5001,POL-QA-5002
    -> {"policies": [{...}, {...}]}

It also accepts (and ignores) an Authorization header, so the demo can show
the pipeline sending a credential from its Airflow connection.

Standard library only, so it runs on a bare python:3.12-slim image.
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

# Policies_QA, verbatim (Excel serial dates decoded).
POLICIES = {
    "POL-QA-5001": {"policy_id": "POL-QA-5001", "customer_id": "CUS-QA-1001",
                    "policy_type": "AUTO", "coverage_limit": 50000,
                    "effective_date": "2026-01-01", "expiration_date": "2026-12-31",
                    "policy_status": "ACTIVE"},
    "POL-QA-5002": {"policy_id": "POL-QA-5002", "customer_id": "CUS-QA-1002",
                    "policy_type": "HOME", "coverage_limit": 350000,
                    "effective_date": "2026-03-01", "expiration_date": "2027-02-28",
                    "policy_status": "ACTIVE"},
    "POL-QA-5003": {"policy_id": "POL-QA-5003", "customer_id": "CUS-QA-1003",
                    "policy_type": "AUTO", "coverage_limit": 75000,
                    "effective_date": "2026-05-15", "expiration_date": "2027-05-14",
                    "policy_status": "ACTIVE"},
    "POL-QA-5004": {"policy_id": "POL-QA-5004", "customer_id": "CUS-QA-1004",
                    "policy_type": "HOME", "coverage_limit": 500000,
                    "effective_date": "2026-02-10", "expiration_date": "2027-02-09",
                    "policy_status": "ACTIVE"},
    "POL-QA-5005": {"policy_id": "POL-QA-5005", "customer_id": "CUS-QA-1005",
                    "policy_type": "RENTERS", "coverage_limit": 50000,
                    "effective_date": "2026-07-01", "expiration_date": "2027-06-30",
                    "policy_status": "ACTIVE"},
}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            return self._json({"status": "ok"})

        if parsed.path == "/policies":
            requested = parse_qs(parsed.query).get("policy_ids", [""])[0]
            ids = [p.strip() for p in requested.split(",") if p.strip()]
            found = [POLICIES[p] for p in ids if p in POLICIES]
            auth = "yes" if self.headers.get("Authorization") else "no"
            print(f"  policy-api: {len(ids)} requested, {len(found)} found "
                  f"(authenticated: {auth})", flush=True)
            return self._json({"policies": found})

        self.send_response(404)
        self.end_headers()

    def _json(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass            # the handler prints its own one-line summary


if __name__ == "__main__":
    print("policy-api listening on :8000", flush=True)
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
