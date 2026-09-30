from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

jdbc_url = (
    "jdbc:postgresql://"
    "dea.cgyi97rb4alr.us-east-1.rds.amazonaws.com:5432/"
    "dea_analytics_dev"
)
username = "student_user"
# Replace only in your private Glue editor copy; never commit a real password.
password = "YOUR_POSTGRES_PASSWORD"
if password == "YOUR_POSTGRES_PASSWORD":
    raise ValueError("Set the PostgreSQL password in your private runtime copy.")

query = """
(
    SELECT
        raw_data,
        insert_date
    FROM raw.lead_activites_raw
    LIMIT 100
) AS source_data
"""

print("Connecting to PostgreSQL...")
df = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", query)
    .option("user", username)
    .option("password", password)
    .option("driver", "org.postgresql.Driver")
    .load()
)
print("===== CONNECTION SUCCESSFUL =====")
print("Row count:", df.count())
print("===== SCHEMA =====")
df.printSchema()
print("===== SAMPLE DATA =====")
df.show(5, truncate=False)
