from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

# Create Spark Session
spark = SparkSession.builder.appName("ETL_Project").getOrCreate()

# Load CSV
df = spark.read.csv("data.csv", header=True, inferSchema=True)

print("Original Data")
df.show()

# Data Cleaning
cleaned_df = df.dropna()

print("Cleaned Data")
cleaned_df.show()

# Filtering
filtered_df = cleaned_df.filter(col("salary") > 55000)

print("Filtered Data")
filtered_df.show()

# Aggregation
avg_salary_df = cleaned_df.groupBy("department").agg(avg("salary"))

print("Average Salary")
avg_salary_df.show()

# Count
count_df = cleaned_df.groupBy("department").agg(count("id"))

print("Employee Count")
count_df.show()

# Save Output
avg_salary_df.write.csv("output", header=True, mode="overwrite")

print("ETL Process Completed!")

spark.stop()