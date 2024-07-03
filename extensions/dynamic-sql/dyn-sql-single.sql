SELECT
  i + 1 AS "index sum",
  i AS "Index"
FROM
  generate_series(1, 10) i;
