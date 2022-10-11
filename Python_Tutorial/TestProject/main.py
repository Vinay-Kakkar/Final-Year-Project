from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("reviewerId", StringType(), True),
    StructField("reviewerName", StringType(), True),
    StructField("reviewText", StringType(), True),
    StructField("overall",StringType(),True),
    StructField("summary", StringType(), True),
])

data = spark\
    .read\
    .schema(schema)\
    .json("data.json")

data.printSchema()