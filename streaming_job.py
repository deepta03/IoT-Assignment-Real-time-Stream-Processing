from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType
from pyspark.sql.functions import col, avg, window, lag, lit
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("HospitalPatientMonitoring")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

stream_directory = "stream_input"
checkpoint_directory = "checkpoints_hospital_monitoring"

schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("patient_number", IntegerType(), True),
    StructField("heart_rate", DoubleType(), True),
])

# Reading streaming files from the watched directory
patient_stream = (
    spark.readStream
    .schema(schema)
    .option("header", True)
    .option("maxFilesPerTrigger", 1)
    .csv(stream_directory)
)

# Computing average heart rate per patient in 2-minute tumbling windows
windowed_hr = (
    patient_stream
    .withWatermark("timestamp", "5 minutes")
    .groupBy(
        window(col("timestamp"), "2 minutes").alias("hr_window"),
        col("patient_number")
    )
    .agg(avg("heart_rate").alias("avg_hr"))
)

# Detecting patients with average HR > 100 in two consecutive windows
def detect_alerts(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    w = Window.partitionBy("patient_number").orderBy("window_start")

    alerts = (
        batch_df
        .select(
            col("patient_number"),
            col("hr_window.start").alias("window_start"),
            col("hr_window.end").alias("window_end"),
            col("avg_hr")
        )
        .withColumn("previous_avg_hr", lag("avg_hr").over(w))
        .filter((col("avg_hr") > 100) & (col("previous_avg_hr") > 100))
        .select(
            col("patient_number"),
            col("window_start"),
            col("window_end"),
            col("previous_avg_hr"),
            col("avg_hr"),
            lit("SUSTAINED_ELEVATED_HEART_RATE").alias("alert_type")
        )
    )
    if not alerts.isEmpty():
        print("\n" + "=" * 80)
        print(f"SUSTAINED HEART RATE ALERT DETECTED IN BATCH {batch_id}")
        print("=" * 80)
        alerts.show(truncate=False)
    
    

query = (
    windowed_hr
    .writeStream
    .foreachBatch(detect_alerts)
    .outputMode("complete")
    .option("checkpointLocation", checkpoint_directory)
    .trigger(processingTime="5 seconds")
    .start()
)

print("Hospital patient monitoring stream started.")
print("Waiting for clinical alerts...")

query.awaitTermination()