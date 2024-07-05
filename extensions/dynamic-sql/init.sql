-- test cases
select 'qdr',3,4;

select 'a', 3, i from generate_series(1,10) i;


-- curl
-- curl -d "@dyn-sql-data-file.sql" -X POST localhost:8080

CREATE OR REPLACE FUNCTION test_update_dynamic_sql (query text)
  RETURNS jsonb
  AS $$
DECLARE
  json_ret jsonb;
  rec record;
  statements setof raw_statement;
BEGIN
  json_ret := '[]'::jsonb;
  -- https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING
  FOR rec IN EXECUTE query
  LOOP
    json_ret := json_ret || to_jsonb(rec);
  END LOOP;
-- EXCEPTION WHEN invalid_cursor_definition then
  RETURN json_ret;
END;
$$
LANGUAGE plpgsql;

-- sql
CREATE OR REPLACE FUNCTION execute_sql_statement (sql_string_request text)
  RETURNS jsonb
AS $pgsql$
  DECLARE
  json_ret jsonb;
  rec record;
  stmt_row_count bigint;
BEGIN
  BEGIN
    json_ret := '[]'::jsonb;
    -- https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING
    RAISE NOTICE 'Trying to evaluate a single statement that should return a data set...';
    FOR rec IN EXECUTE sql_string_request LOOP
      RAISE NOTICE 'statement % generated row (%)', sql_string_request, rec;
      json_ret := json_ret || to_jsonb (rec);
    END LOOP;
    RETURN json_ret;
    -- https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    -- trapping errors
  EXCEPTION
    WHEN invalid_cursor_definition THEN
      RAISE NOTICE 'Trying to evaluate a single statement that should return a single datum...';
      BEGIN
        EXECUTE sql_string_request INTO rec;
        RETURN rec::jsonb;
      EXCEPTION
        WHEN syntax_error THEN
          RAISE NOTICE 'Trying to evaluate a single statement that should not return anything...';
          GET DIAGNOSTICS stmt_row_count = ROW_COUNT;
          EXECUTE sql_string_request;
          RETURN format($ret_message$"%s rows processed by the command '%s'"$ret_message$, stmt_row_count, sql_string_request)::jsonb;
      END;
  END;
END;
$pgsql$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION execute_sql_statement_set (statements setof omni_sql.raw_statement[])
  RETURNS jsonb
  AS $pgsql$
DECLARE
  return_json jsonb;
  stmt omni_sql.raw_statement;
BEGIN
  return_json := '[]'::jsonb;
  FOREACH stmt IN ARRAY statements LOOP
    DECLARE stmt_json jsonb;
    BEGIN
      return_json := execute_sql_statement (stmt);
    END;
  END LOOP;
  RETURN return_json;
END;
$pgsql$
LANGUAGE plpgsql;

update omni_httpd.handlers
   set query = $$select omni_httpd.http_response(update_dynamic_sql(convert_from(request.body, 'UTF8'))) from request where request.method = 'POST'$$

-- tests
-- https://stackoverflow.com/a/43979964
SELECT  execute_sql_statement_set(array_agg(raw_statements)) from omni_sql.raw_statements (
                   $stmt_set$
 DROP SCHEMA IF EXISTS dyn_sql CASCADE;

CREATE SCHEMA IF NOT EXISTS dyn_sql;

CREATE TABLE IF NOT EXISTS dyn_sql.test_table (
  id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  posted_at timestamp DEFAULT now()
);

INSERT INTO dyn_sql.test_table DEFAULT VALUES;
$stmt_set$);
