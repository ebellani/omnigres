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
CREATE OR REPLACE FUNCTION update_dynamic_sql (sql_string_request text)
  RETURNS jsonb
AS $body$
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
          RETURN format('% rows processed by the command %', stmt_row_count, sql_string_request)::jsonb;
      END;
  END;
END;
$body$
  LANGUAGE plpgsql;




update omni_httpd.handlers
   set query = $$select omni_httpd.http_response(update_dynamic_sql(convert_from(request.body, 'UTF8'))) from request where request.method = 'POST'$$
