DAILY INSURANCE CLAIMS PROCESSING (plain-language version of README.md)

WHAT IT IS
A program that runs every morning at 2 AM New York time and handles the previous day's insurance claims. It uses Apache Airflow (a job scheduler). It was built to a requirements document supplied by the client, which is kept in assignment-instructions/. The file FINAL-REPORT.md lines up every requirement with the code and the test that covers it.

HOW A DAY FLOWS
A claims spreadsheet (CSV) arrives in cloud storage. The program checks every claim, looks up the customer in a database and the policy through a web service, gives each claim a risk number, and sends the good claims to a Kafka message queue for other teams. Claims with problems are saved to a separate error folder with a plain reason. Both paths run side by side. Then the program checks that the numbers add up, writes a daily summary row to the database, moves the original file to an archive folder, and sends a notification.

WHAT IS NOT INCLUDED
The storage, database, web service and Kafka belong to the client. For demos, the demo folder starts four stand-in programs in Docker containers. The automated tests use fakes instead.

ALL THE DATA IS MADE UP
Every customer, policy and claim in this repository comes from the QA test workbook and uses fake IDs such as CUS-QA-1001. No real claimant data is in it.

THINGS THAT ARE NOT REAL YET
The risk number is a placeholder. The client never said how it should be calculated, so the program uses a simple labelled stand-in, and its values will not match the numbers in the client's workbook. Notifications are only written to the log. Nothing is sent by Slack, email or pager.

WHAT YOU NEED
Python 3.12 (3.13 or newer does not work with this Airflow version). For the demo you also need Docker Desktop running, and ports 19000, 19001, 15432, 19092 and 18000 free. You do not need to install Kafka, Postgres, S3 or Airflow yourself.

HOW TO RUN THE DEMO
1. Start the four stand-in services: docker compose -f demo/docker-compose.yml up -d. The first time it downloads images. Wait until docker ps --filter name=claimsdemo shows four healthy containers.
2. Run ./demo/setup.sh. It builds the Python environment, creates the storage bucket, registers the connections and uploads the sample claims file.
3. Run ./demo/run.sh. It runs the pipeline once and prints what each step did, the messages sent to Kafka, the summary row and whether the file was archived.
4. To see the bad-claims case, run ./demo/setup.sh rejects and then ./demo/run.sh again.
5. To clean up, run docker compose -f demo/docker-compose.yml down -v.
If a port is already in use, change it in demo/docker-compose.yml and demo/env.sh. If you see "Connection with ID aws_default not found", run ./demo/setup.sh again. More detail is in FOLLOW-THESE-STEPS.md and demo/README.md.

WHAT IT DOES IN EACH SITUATION
A good claim is enriched, scored and sent to Kafka.
A bad claim (bad amount, unknown customer or policy, missing field, duplicate ID) goes to the error folder with a reason, and the run still counts as a success. A bad claim is normal business, not an outage.
A real failure (file missing or empty, wrong columns, database or Kafka down) makes the run fail, leaves the input file where it was so the day can be re-run, and raises a failure alert.
If more than 10 percent of claims are rejected, an extra alert is raised. The threshold is the setting CLAIMS_REJECTION_RATE_THRESHOLD.
When the counts add up, the summary row is written and the file is archived.

TECHNICAL FACTS
The job is called daily_insurance_claims_processing. It runs at 0 2 * * * New York time, one run at a time. To process a particular date by hand, pass processing_date as YYYY-MM-DD in the run settings. Good claims go to the Kafka topic insurance.claims.processed, with insurance.claims.dlq as the dead-letter topic. Settings are environment variables defined in claims/common/config.py. Passwords come from Airflow connections.

TESTS
Create the environment with python3.12 -m venv .venv and .venv/bin/pip install -r requirements.txt, then run .venv/bin/python -m pytest tests/ -q. Expected result: 229 passed. No Docker or outside services are needed. The tests cover validation, enrichment, risk scoring, Kafka retries, the count reconciliation, notification text, the job's step layout, the failure-case fixtures in qa/, and one end-to-end run. The automatic checks on GitHub (ci.yml) run the same command and also scan the full history for leaked passwords. The Docker demo is not run automatically.

WHERE THINGS ARE
dags/insurance_claims_dag.py is the job definition and only coordinates. The folder claims/ holds the real logic: common (settings, storage, lookups), validate, enrich, score, publish, load and notify. tests/ holds the 229 tests. qa/ holds extra failure-case sample files. demo/ holds the Docker stand-ins and scripts. assignment-instructions/ holds the client's documents. docs/ holds the architecture notes and ten open questions for the client. FINAL-REPORT.md maps requirements to code and tests.

WHY IT IS BUILT THIS WAY
A bad claim is treated as data, and a broken system is treated as an error. Raising errors for every bad claim would page engineers for ordinary data-quality noise, and treating outages as bad claims would hide lost data.
Bad claims are saved at the same time as good claims are published, so a Kafka outage cannot lose the record of rejections.
Archiving happens only after the counts reconcile, because the original file is the only copy that can be re-run.
Claim details are kept out of Airflow's message passing because they are personal data. They move through dated staging files instead, which also lets a single step be re-run by hand.
