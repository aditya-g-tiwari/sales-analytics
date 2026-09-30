"""Load one complete manifest. Run only one loader at a time.

AWS credentials use the normal SDK chain. Snowflake uses browser SSO.
No PostgreSQL credentials are needed by this loader.
"""
import argparse
import json
import os
import re
from datetime import datetime
from urllib.parse import urlparse

BASE = 's3://sales-analytics-aditya-task-2/raw/'
TABLES = {
    'raw.leads_raw': 'leads', 'raw.lead_activites_raw': 'lead_activities',
    'raw.close_crm_users_raw': 'close_crm_users', 'raw.custom_activites_raw': 'custom_activities',
}

def validate_manifest(manifest):
    run_id = manifest.get('run_id', '')
    if not isinstance(run_id, str) or not re.fullmatch(r'[0-9a-f]{32}', run_id):
        raise ValueError('Invalid run ID')
    if manifest.get('version') != 1 or manifest.get('status') != 'complete':
        raise ValueError('Only complete version 1 manifests can be loaded')
    if manifest.get('s3_base_path') != BASE:
        raise ValueError('Unexpected S3 base path')
    for field in ('started_at', 'completed_at'):
        value = datetime.fromisoformat(manifest[field])
        if value.tzinfo is None:
            raise ValueError('Manifest timestamps must include a timezone')
    if datetime.fromisoformat(manifest['completed_at']) < datetime.fromisoformat(manifest['started_at']):
        raise ValueError('Invalid timestamp order')
    entries = manifest.get('tables', [])
    if len(entries) != 4 or {x['source_table'] for x in entries} != set(TABLES):
        raise ValueError('Expected each of the four sources exactly once')
    for entry in entries:
        expected = f"{TABLES[entry['source_table']]}/run_id={run_id}/"
        if entry['relative_path'] != expected:
            raise ValueError('Unexpected stage path')
        if type(entry['row_count']) is not int or entry['row_count'] < 0:
            raise ValueError('Expected a nonnegative integer count')
    return manifest

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', required=True, help='S3 URI printed by the Glue job')
    args = parser.parse_args()
    if not re.fullmatch(re.escape(BASE) + r'_manifests/[0-9a-f]{32}\.json', args.manifest):
        raise ValueError('Manifest must be under the configured raw/_manifests prefix')
    import boto3
    import snowflake.connector
    parsed = urlparse(args.manifest)
    manifest = validate_manifest(json.loads(boto3.client('s3').get_object(
        Bucket=parsed.netloc, Key=parsed.path.lstrip('/'))['Body'].read()))
    run_id = manifest['run_id']
    if not args.manifest.endswith('/' + run_id + '.json'):
        raise ValueError('Manifest filename and run ID do not match')
    connection = snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'], user=os.environ['SNOWFLAKE_USER'],
        authenticator='externalbrowser', role=os.environ['SNOWFLAKE_ROLE'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'], database='SALES_ANALYTICS', schema='BRONZE',
        session_parameters={'TIMEZONE': 'UTC', 'QUERY_TAG': 'sales-analytics-manifest-load'},
    )
    try:
        with connection.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM BRONZE.INGESTION_RUNS WHERE RUN_ID=%s', (run_id,))
            if cur.fetchone()[0]:
                print('This run is already committed; no changes made.')
                return
            # Temporary landing is invisible to reporting. DDL stays outside the transaction.
            cur.execute('CREATE TEMPORARY TABLE LOAD_BUFFER LIKE BRONZE.RAW_RECORDS')
            for entry in manifest['tables']:
                # Paths can only match a fixed prefix and validated hex run ID.
                path = entry['relative_path']
                cur.execute(f"""
                    COPY INTO LOAD_BUFFER
                    (SOURCE_TABLE, RUN_ID, RAW_DATA, INSERT_DATE_TEXT, EXTRACTED_AT,
                     SOURCE_FILE, SOURCE_FILE_ROW, LOADED_AT)
                    FROM (SELECT $1:source_table::STRING, $1:run_id::STRING,
                                 $1:raw_data::STRING, $1:insert_date::STRING,
                                 $1:extracted_at::TIMESTAMP_TZ,
                                 METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
                          FROM @BRONZE.S3_RAW_STAGE/{path})
                    FILE_FORMAT=(FORMAT_NAME='BRONZE.RAW_JSON')
                    PATTERN='.*[.]json[.]gz' ON_ERROR='ABORT_STATEMENT' FORCE=TRUE
                """)
            cur.execute('SELECT SOURCE_TABLE, RUN_ID, COUNT(*) FROM LOAD_BUFFER GROUP BY 1,2')
            actual = {(table, run): count for table, run, count in cur.fetchall()}
            expected = {(x['source_table'], run_id): x['row_count'] for x in manifest['tables']
                        if x['row_count'] > 0}
            if actual != expected:
                raise ValueError(f'Source counts do not match the manifest: {actual} != {expected}')
            cur.execute("""SELECT COUNT(*) FROM (
                SELECT SOURCE_FILE, SOURCE_FILE_ROW FROM LOAD_BUFFER
                GROUP BY 1,2 HAVING COUNT(*) > 1)""")
            if cur.fetchone()[0]:
                raise ValueError('Duplicate file/row identities in staging')
            # All four sources, including empty sources, become visible together.
            cur.execute('BEGIN')
            try:
                cur.execute('INSERT INTO BRONZE.RAW_RECORDS SELECT * FROM LOAD_BUFFER')
                cur.execute("""INSERT INTO BRONZE.INGESTION_RUNS
                    (RUN_ID, STARTED_AT, COMPLETED_AT, MANIFEST_URI, MANIFEST, LOADED_AT)
                    SELECT %s, TO_TIMESTAMP_TZ(%s), TO_TIMESTAMP_TZ(%s), %s,
                           PARSE_JSON(%s), CURRENT_TIMESTAMP()""",
                    (run_id, manifest['started_at'], manifest['completed_at'],
                     args.manifest, json.dumps(manifest)))
                cur.execute('COMMIT')
            except Exception:
                cur.execute('ROLLBACK')
                raise
        print(f'Committed snapshot {run_id}; Silver/Gold/reporting views now reflect the latest snapshot.')
    finally:
        connection.close()

if __name__ == '__main__':
    main()
