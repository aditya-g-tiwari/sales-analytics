-- Connect to dea_analytics_dev as student_user.
-- Reconstructed equivalents of the discovery checks recorded in the chat.
-- Source spellings (activites) are intentional. These queries are read-only.
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'raw'
  AND table_name IN ('leads_raw', 'lead_activites_raw',
                     'close_crm_users_raw', 'custom_activites_raw')
ORDER BY table_name;

SELECT table_schema, table_name, column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_schema = 'raw'
  AND table_name IN ('leads_raw', 'lead_activites_raw',
                     'close_crm_users_raw', 'custom_activites_raw')
ORDER BY table_name, ordinal_position;

SELECT 'raw.leads_raw' AS source_table, COUNT(*) AS row_count FROM raw.leads_raw
UNION ALL
SELECT 'raw.lead_activites_raw', COUNT(*) FROM raw.lead_activites_raw
UNION ALL
SELECT 'raw.close_crm_users_raw', COUNT(*) FROM raw.close_crm_users_raw
UNION ALL
SELECT 'raw.custom_activites_raw', COUNT(*) FROM raw.custom_activites_raw;

-- Inspect original JSON/text without flattening arrays or decoding wrappers.
SELECT raw_data, insert_date FROM raw.leads_raw LIMIT 5;
SELECT raw_data, insert_date FROM raw.lead_activites_raw LIMIT 5;
SELECT raw_data, insert_date FROM raw.close_crm_users_raw LIMIT 5;
SELECT raw_data, insert_date FROM raw.custom_activites_raw LIMIT 5;
-- Observed: lead_activites_raw contains data[] arrays;
-- close_crm_users_raw and custom_activites_raw contain JSON_OBJECT wrappers
-- or stringified content. Preserve these structures at this stage.
