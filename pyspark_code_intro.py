from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.master("local[*]").appName("PySparkApp").getOrCreate()

# Sample data
data = [("John", 25), ("Doe", 29), ("Jane", 30)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show DataFrame
df.show()

# Stop the Spark session
spark.stop()

