# Daily Insurance Claims Processing

## Recruiter-friendly overview
This project automates a daily business process that insurance teams commonly perform: finding a claims file, checking every record, adding customer and policy information, separating valid claims from records that need attention, and notifying the right people. It treats normal data-quality issues differently from system failures, so teams know whether to review a claim or fix the platform. The project demonstrates workflow automation, reliable data processing, testing, messaging, and clear operational communication.

## Project description
An Apache Airflow pipeline that validates, enriches, risk-scores, publishes, reconciles, and archives daily insurance claims while providing clear handling for rejected records and infrastructure failures.

---

# Daily insurance claims processing

An Apache Airflow pipeline that processes your claims file every morning:
it checks the file, validates every claim, fills in customer and policy
details, scores each claim for risk, and publishes the good ones to Kafka for
your downstream teams.

Built to `assignment-instructions/airflow_insurance_claims_dag_requirements.pdf`.

---

## Quick start

You need **Python 3.12** (Airflow 3.0.2 does not yet support 3.13).

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest tests/ -q
```

You should see **229 passed**. The rest of the original setup, demo, workflow, testing, and deployment notes remain below.
