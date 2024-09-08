from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, IntegerType, FloatType


KEYFILE_PATH = "/opt/spark/pyspark/deb4-day3-spark-key.json"

# GCS Connector Path (on Spark): /opt/spark/jars/gcs-connector-hadoop3-latest.jar
# GCS Connector Path (on Airflow): /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/gcs-connector-hadoop3-latest.jar
# spark = SparkSession.builder.appName("demo") \
#     .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
#     .config("spark.memory.offHeap.enabled", "true") \
#     .config("spark.memory.offHeap.size", "5g") \
#     .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("google.cloud.auth.service.account.enable", "true") \
#     .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
#     .getOrCreate()

spark = SparkSession.builder.appName("transform") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

# schema for Greenery data

struct_schema_dict = {
    'addresses':StructType([
    StructField("address_id", StringType()),
    StructField("address", StringType()),
    StructField("zipcode", StringType()),
    StructField("state", StringType()),
    StructField("country", StringType())
    ]),
    'events':StructType([
    StructField("event_id", StringType()),
    StructField("session_id", StringType()),
    StructField("page_url", StringType()),
    StructField("created_at", TimestampType()),
    StructField("event_type", StringType()),
    StructField("user", StringType()),
    StructField("order", StringType()),
    StructField("product", StringType()),
    ]),
    'order-items':StructType([
    StructField("order_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    ]),
    'orders':StructType([
    StructField("id", StringType()),
    StructField("promo_id", StringType()),
    StructField("address_id", StringType()),
    StructField("created_at", TimestampType()),
    StructField("order_cost", FloatType()),
    StructField("shipping_cost", FloatType()),
    StructField("order_total", FloatType()),
    StructField("tracking_id", StringType()),
    StructField("shipping_service", StringType()),
    StructField("estimated_delivery_at", TimestampType()),
    StructField("delivered_at", TimestampType()),
    StructField("status", StringType()),
    ]),
    'products':StructType([
    StructField("product_id", StringType()),
    StructField("name", StringType()),
    StructField("price", FloatType()),
    StructField("inventory", StringType()),
    ]),
    'promos':StructType([
    StructField("promo_id", StringType()),
    StructField("discount", IntegerType()),
    StructField("status", StringType()),
    ]),
    'users':StructType([
        StructField("user_id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("email", StringType()),
        StructField("phone_number", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("address_id", StringType()),
    ])
}    


data_filename_list=[
    'addresses',
    'events',
    'order-items',
    'orders',
    'products',
    'promos',
    'users'
]

for data_filename in data_filename_list:
    GCS_FILE_PATH = f"gs://deb4-bootcamp-004/raw/greenery/{data_filename}/{data_filename}.csv"

    # df = spark.read \
    #     .option("header", True) \
    #     .option("inferSchema", True) \
    #     .csv(GCS_FILE_PATH)

    df = spark.read \
        .option("header", True) \
        .schema(struct_schema_dict[data_filename]) \
        .csv(GCS_FILE_PATH)

    df.show()
    viewname = data_filename.replace('-','_')
    df.createOrReplaceTempView(viewname)
    result = spark.sql(f"""
        select
            *

        from {viewname}
    """)

    OUTPUT_PATH = f"gs://deb4-bootcamp-004/processed/greenery/{data_filename}/{data_filename}.csv"
    result.write.mode("overwrite").parquet(OUTPUT_PATH)