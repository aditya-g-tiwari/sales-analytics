-- Administrator setup. Replace the role ARN before running.
CREATE STORAGE INTEGRATION IF NOT EXISTS SALES_ANALYTICS_S3_INT
  TYPE=EXTERNAL_STAGE STORAGE_PROVIDER='S3' ENABLED=TRUE
  STORAGE_AWS_ROLE_ARN='arn:aws:iam::<AWS_ACCOUNT_ID>:role/<SNOWFLAKE_S3_ROLE>'
  STORAGE_ALLOWED_LOCATIONS=('s3://sales-analytics-aditya-task-2/raw/');
DESC INTEGRATION SALES_ANALYTICS_S3_INT;
-- Use STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from DESC
-- to complete aws/snowflake-trust-policy.example.json in IAM before continuing.
