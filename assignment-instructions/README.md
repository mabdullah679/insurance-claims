# Assignment instructions (client-supplied)

Copies of the three artefacts the client provided. The code in this repository
is built against these and nothing else. `FINAL-REPORT.md` in the repository
root maps every requirement in the PDF to the code that implements it and to
the test that proves it.

| File | What it is |
|---|---|
| `airflow_insurance_claims_dag_requirements.pdf` | The requirements package (8 pages, prepared 12 September 2026): schedule, input sources, the 15 workflow steps, the Kafka message contract, failure handling, data-quality rules, monitoring metrics and alert conditions, the notification table, and the stakeholder impact matrix. |
| `insurance_claims_qa_test_data_20260911.xlsx` | The QA workbook (6 tabs): `QA Guide`, `Claims_Input`, `Customers_QA`, `Policies_QA`, `Expected_Kafka`, `Audit`. One happy-path scenario for processing date 2026-09-11: five claims, zero rejections, five Kafka acknowledgements. |
| `mermaid-diagram.png` | The people and business-impact flow: who uploads the file, who is notified on success, who reviews record rejections, and who is paged on DAG failure. |

The CSV form of `Claims_Input` is checked in as `qa/fixtures/claims_20260911_happy.csv`,
and the `Customers_QA` / `Policies_QA` tabs are reproduced verbatim as the
fakes in `tests/conftest.py`.
