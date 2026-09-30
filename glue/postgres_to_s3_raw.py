import json
import sys
import uuid
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark import StorageLevel
from pyspark.sql import functions as F

TABLES = {
    'raw.leads_raw': 'leads',
    'raw.lead_activites_raw': 'lead_activities',
    'raw.close_crm_users_raw': 'close_crm_users',
    'raw.custom_activites_raw': 'custom_activities',
}

def s3_parts(uri):
    parsed = urlparse(uri)
    if parsed.scheme != 's3' or not parsed.netloc or not parsed.path.strip('/'):
        raise ValueError('Expected an S3 URI with a bucket and key')
    return parsed.netloc, parsed.path.lstrip('/')

def main():
    names = ['JOB_NAME', 'CONFIG_S3_URI']
    if '--TEST_ONLY' in sys.argv:
        names.append('TEST_ONLY')
    args = getResolvedOptions(sys.argv, names)
    test_value = args.get('TEST_ONLY', 'false').lower()
    if test_value not in ('true', 'false'):
        raise ValueError('TEST_ONLY must be true or false')
    test_only = test_value == 'true'
    s3 = boto3.client('s3')
    bucket, key = s3_parts(args['CONFIG_S3_URI'])
    config = json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read())
    if config['tables'] != TABLES:
        raise ValueError('This version expects exactly the four configured sources')
    if config['s3_base_path'] != 's3://sales-analytics-aditya-task-2/raw/':
        raise ValueError('Update Glue and Snowflake configuration together before changing the base path')
    secret = json.loads(boto3.client('secretsmanager', region_name=config['region'])
                        .get_secret_value(SecretId=config['secret_id'])['SecretString'])
    if not secret.get('password') or secret['password'].startswith('REPLACE_'):
        raise ValueError('Set a real password in Secrets Manager')
    sslmode = config['jdbc_sslmode']
    if sslmode not in ('require', 'verify-ca', 'verify-full'):
        raise ValueError('Use an encrypted PostgreSQL JDBC connection')
    url = (f"jdbc:postgresql://{config['postgres_host']}:{int(config['postgres_port'])}/"
           f"{config['postgres_database']}")
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    job = Job(glue)
    job.init(args['JOB_NAME'], args)
    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc).isoformat()
    results = []
    sources = {'raw.lead_activites_raw': TABLES['raw.lead_activites_raw']} if test_only else TABLES
    for table, prefix in sources.items():
        # Cast to text in PostgreSQL to preserve wrappers and source timestamp text.
        query = (f'(SELECT raw_data::text AS raw_data, insert_date::text AS insert_date '
                 f'FROM {table}' + (' LIMIT 100' if test_only else '') + ') AS source_data')
        frame = (spark.read.format('jdbc').option('url', url).option('dbtable', query)
                 .option('user', secret['username']).option('password', secret['password'])
                 .option('driver', 'org.postgresql.Driver').option('sslmode', sslmode)
                 .option('fetchsize', '10000').load())
        frame.persist(StorageLevel.DISK_ONLY)
        try:
            count = frame.count()
            print(f'{table}: {count} source rows; run_id={run_id}')
            frame.printSchema()
            if not test_only:
                relative = f'{prefix}/run_id={run_id}/'
                envelope = (frame.withColumn('source_table', F.lit(table))
                            .withColumn('run_id', F.lit(run_id))
                            .withColumn('extracted_at', F.lit(started_at)))
                (envelope.write.mode('errorifexists').option('compression', 'gzip')
                 .option('ignoreNullFields', 'false').json(config['s3_base_path'] + relative))
                results.append({'source_table': table, 'relative_path': relative, 'row_count': count})
        finally:
            frame.unpersist()
    if not test_only:
        manifest = {'version': 1, 'status': 'complete', 'run_id': run_id,
                    'started_at': started_at, 'completed_at': datetime.now(timezone.utc).isoformat(),
                    's3_base_path': config['s3_base_path'], 'tables': results}
        manifest_uri = config['s3_base_path'] + f'_manifests/{run_id}.json'
        bucket, key = s3_parts(manifest_uri)
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(manifest).encode(),
                      ContentType='application/json')
        print(f'Complete snapshot manifest: {manifest_uri}')
    job.commit()

if __name__ == '__main__':
    main()
