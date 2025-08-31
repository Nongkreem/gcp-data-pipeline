# แปลงจาก .ipynb เป็น .py แล้ว

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, concat_ws, lit, year, month, dayofmonth, weekofyear, quarter
import sys

# สร้าง Spark session
spark = SparkSession.builder \
    .appName("etl to BigQuery") \
    .getOrCreate()
    
# --- READ CSV FROM GCS ---
bucket_path = "gs://gcs-data-marketing-463102/notebooks/jupyter/raw_data/bank.csv" 

# อ่าน CSV จาก GCS
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(bucket_path)

#TRANSFORM: Standardize data
# สร้าง contact_date จาก day + month (สมมุติปี 2024)
df_with_date = df.withColumn("contact_date",
    to_date(concat_ws("-", lit("2024"), col("month"), col("day")), "yyyy-MMM-d")
)
df_clean = df \
    .withColumn("job", lower(trim(col("job")))) \
    .withColumn("marital", lower(trim(col("marital")))) \
    .withColumn("education", lower(trim(col("education")))) \
    .withColumn("month", lower(trim(col("month")))) \
    .withColumn("contact_date", to_date(concat_ws("-", lit("2024"), col("month"), col("day")), "yyyy-MMM-d"))

#DEBUGGING: Check data quality
print("==== Schema ====")
df_clean.printSchema()

print("==== Sample Records ====")
df_clean.show(5)

print("==== Null Count Per Column ====")
null_counts = df_clean.select([col(c).isNull().cast("int").alias(c) 
					for c in df_clean.columns]) \
					.groupBy().sum()
null_counts.show()

print("==== Check date ====")
df_clean.filter((col("month") == "feb") & (col("day") == 12)) \
        .select("month", "day", "contact_date") \
        .show(truncate=False)

# WRITE PARQUET FILES TO GCS BY MONTH
output_bucket_path = "gs://gcs-data-marketing-463102/notebooks/jupyter/clean_data"

# Save data partitioned by month
df_clean.write \
    .partitionBy("month") \
    .mode("overwrite") \
    .parquet(output_base_path)

# LOAD CLEAN DATA INTO BIGQUERY
bucket_name = "gcs-data-marketing-463102"

df_clean.write \
    .format("bigquery") \
    .option("table", "my-spark-project-463102.bank_data.bank") \
    .option("temporaryGcsBucket", bucket_name) \
    .mode("overwrite") \
    .save()

spark.stop()