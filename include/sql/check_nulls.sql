SELECT '{table_name}' AS table_name, COALESCE(COUNT(*), 0) AS num_rows
FROM `{project_id}.{dataset_id}.{table_name}`
