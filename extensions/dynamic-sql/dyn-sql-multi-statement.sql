DROP SCHEMA IF EXISTS dyn_sql CASCADE;

CREATE SCHEMA IF NOT EXISTS dyn_sql;

CREATE TABLE IF NOT EXISTS dyn_sql.test_table (
  id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  posted_at timestamp DEFAULT now()
);

INSERT INTO dyn_sql.test_table DEFAULT VALUES;
SELECT
  *
FROM
  omni_sql.raw_statements (E' Select True;\n select 1'
)

-- parse bunch of expressions
 SELECT
  *
FROM
  omni_sql.raw_statements ('DROP SCHEMA IF EXISTS dyn_sql CASCADE;

CREATE SCHEMA IF NOT EXISTS dyn_sql;

CREATE TABLE IF NOT EXISTS dyn_sql.test_table (
  id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  posted_at timestamp DEFAULT now()
);

INSERT INTO dyn_sql.test_table DEFAULT VALUES;');
