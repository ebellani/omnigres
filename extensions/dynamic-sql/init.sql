-- test cases
select 'qdr',3,4;

select 'a', 3, i from generate_series(1,10) i;


-- curl
-- curl -d "@dyn-sql-data-file.sql" -X POST localhost:8080

-- sql
CREATE OR REPLACE FUNCTION execute_sql_statement (sql_string_request text) RETURNS jsonb
AS $pgsql$
DECLARE
  json_ret jsonb;
  rec record;
  stmt_row_count bigint;
BEGIN
  BEGIN
    json_ret := '[]'::jsonb;
    -- https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING
    FOR rec IN EXECUTE sql_string_request LOOP
      json_ret := json_ret || to_jsonb (rec);
    END LOOP;
    RETURN json_ret;
    -- https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    -- trapping errors
  EXCEPTION
    WHEN invalid_cursor_definition THEN
      BEGIN
        EXECUTE sql_string_request INTO rec;
        RETURN rec::jsonb;
      EXCEPTION
        WHEN syntax_error THEN
          GET DIAGNOSTICS stmt_row_count = ROW_COUNT;
          EXECUTE sql_string_request;
          RETURN to_json(format($ret_message$%s rows processed by the command %L$ret_message$, stmt_row_count, sql_string_request))::jsonb;
      END;
  END;
END;
$pgsql$ LANGUAGE plpgsql;

COMMENT ON FUNCTION omni_ execute_sql_statement (text) IS
$comment$The underlying mechanism to evaluate arbitrary SQL statements. This uses exceptions for control flow for now, since the kind of SQL statement is not exposed to the pgsql level yet.

Bear in mind that 'all changes to persistent database state within the block are rolled back.'[1]. This should be fine since each statement is executed in a separate call to this function thus evaluating in a separate transaction.

[1] https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
$comment$;

CREATE OR REPLACE FUNCTION execute_sql_statement (statements omni_sql.raw_statement[]) RETURNS jsonb
AS $pgsql$
DECLARE
  return_json jsonb;
  stmt omni_sql.raw_statement;
BEGIN
  return_json := '[]'::jsonb;
  FOREACH stmt IN ARRAY statements LOOP
    DECLARE stmt_json jsonb;
    BEGIN
      return_json := return_json || execute_sql_statement (stmt.source);
    END;
  END LOOP;
  RETURN return_json;
END;
$pgsql$ LANGUAGE plpgsql;

COMMENT ON FUNCTION omni_ execute_sql_statement (text) IS
  $comment$The entry point for evaluating SQL statements.
A way to use it is run something along the lines of  'SELECT execute_sql_statement(array_agg(raw_statements)) from omni_sql.raw_statements (<List of SQL statements as string>)'
$comment$;

update omni_httpd.handlers
   set query = $$select omni_httpd.http_response(update_dynamic_sql(convert_from(request.body, 'UTF8'))) from request where request.method = 'POST'$$

                 -- tests
                 SELECT  execute_sql_statement(array_agg(raw_statements)) from omni_sql.raw_statements (
                   $stmt_set$
 DROP SCHEMA IF EXISTS dyn_sql CASCADE;

CREATE SCHEMA IF NOT EXISTS dyn_sql;

CREATE TABLE IF NOT EXISTS dyn_sql.test_table (
  id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  posted_at timestamp DEFAULT now()
);

INSERT INTO dyn_sql.test_table DEFAULT VALUES;
$stmt_set$);
