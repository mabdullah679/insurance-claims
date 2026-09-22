-- Customer fixtures for the demo, from the QA workbook's `Customers_QA` tab.
-- Loaded automatically when the PostgreSQL container first starts.
--
-- The pipeline reads this table through `PostgresCustomerLookup`. The daily
-- summary table is NOT created here on purpose: the pipeline creates it
-- itself on first run (CREATE TABLE IF NOT EXISTS), which is worth showing.

CREATE TABLE IF NOT EXISTS customers (
    customer_id     TEXT PRIMARY KEY,
    customer_name   TEXT NOT NULL,
    date_of_birth   DATE,
    state           TEXT,
    customer_since  DATE,
    customer_status TEXT NOT NULL
);

INSERT INTO customers (customer_id, customer_name, date_of_birth, state,
                       customer_since, customer_status) VALUES
    ('CUS-QA-1001', 'Olivia Martin',  '1988-04-14', 'GA', '2018-06-05', 'ACTIVE'),
    ('CUS-QA-1002', 'Ethan Walker',   '1979-11-23', 'GA', '2015-02-12', 'ACTIVE'),
    ('CUS-QA-1003', 'Sophia Nguyen',  '1993-08-02', 'FL', '2021-09-19', 'ACTIVE'),
    ('CUS-QA-1004', 'Noah Thompson',  '1985-01-30', 'TX', '2012-12-01', 'ACTIVE'),
    ('CUS-QA-1005', 'Ava Robinson',   '1990-06-17', 'NC', '2020-03-27', 'ACTIVE')
ON CONFLICT (customer_id) DO NOTHING;

-- Note there is deliberately NO customer CUS-QA-9999. The "rejected claim"
-- scenario in the demo relies on that lookup coming back empty, which is how
-- the pipeline distinguishes "this customer does not exist" (reject the one
-- claim) from "the database is down" (fail the whole run).
