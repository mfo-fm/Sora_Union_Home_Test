from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, sqrt, pow

# Step 1: Initialize Spark Session
spark = SparkSession.builder \
    .appName("Big Data Processing with Motion Tracking Dataset") \
    .config("spark.sql.shuffle.partitions", 100) \
    .getOrCreate()

# Step 2: Load the dataset
file_path = r'C:\Users\Farouq.Olaniyan\Documents\Farouq_personal\Sora_Union\benchmark_results.csv'
data = spark.read.csv(file_path, header=True, inferSchema=True)

# Step 3: Inspect the data (optional)
data.printSchema()
data.show(5)

# Step 4: Perform transformations
# a) Aggregate average duration by type and detector
avg_duration = data.groupBy("type", "detector") \
    .agg(avg("duration").alias("avg_duration"))

# b) Calculate average displacement error
# Displacement error = sqrt((displacements_x - displacements_truth_x)^2 + (displacements_y - displacements_truth_y)^2)
data = data.withColumn(
    "displacement_error",
    sqrt(pow(col("displacements_x") - col("displacements_truth_x"), 2) +
         pow(col("displacements_y") - col("displacements_truth_y"), 2))
)
avg_displacement_error = data.groupBy("name") \
    .agg(avg("displacement_error").alias("avg_displacement_error"))

# Step 5: Save results (if needed)
output_path = r'C:\Users\Farouq.Olaniyan\Documents\Farouq_personal\Sora_Union'
avg_duration.write.csv(f"{output_path}/avg_duration", header=True)
avg_displacement_error.write.csv(f"{output_path}/avg_displacement_error", header=True)

# Step 6: Display results
avg_duration.show(5)
avg_displacement_error.show(5)
