-- Schema check for customer_dim table

WITH column_check AS (
  -- Checking if required columns are present
  SELECT column_name
  FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{table_name}'
  AND column_name IN ('customer_key', 'name', 'contact_no', 'nid')
),

column_type_check AS (
  -- Checking column types
  SELECT column_name, data_type
  FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{table_name}'
  AND column_name IN ('customer_key', 'name', 'contact_no', 'nid')
),

expected_columns AS (
  -- Defining the required columns and their expected types
  SELECT 'customer_key' AS column_name, 'STRING' AS expected_type
  UNION ALL
  SELECT 'name', 'STRING'
  UNION ALL
  SELECT 'contact_no', 'INT64'
  UNION ALL
  SELECT 'nid', 'INT64'
),

missing_columns AS (
  -- Check for missing columns by comparing present columns against required ones
  SELECT column_name
  FROM expected_columns
  WHERE column_name NOT IN (SELECT column_name FROM column_check)
),

wrong_column_types AS (
  -- Check for columns with wrong types by comparing the actual types with the expected ones
  SELECT e.column_name
  FROM expected_columns e
  JOIN column_type_check c
    ON e.column_name = c.column_name
  WHERE e.expected_type != c.data_type
)

-- Final output: Pass or Fail based on the checks
SELECT
  CASE
    WHEN (SELECT COUNT(*) FROM missing_columns) > 0 THEN 'FAIL: Missing columns'
    WHEN (SELECT COUNT(*) FROM wrong_column_types) > 0 THEN 'FAIL: Wrong column types'
    ELSE 'PASS'
  END AS schema_check_status;
