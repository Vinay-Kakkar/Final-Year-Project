from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col, countDistinct
from pyspark.sql import functions as func

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("name", StringType(), True),
    StructField("review_count",StringType(),True),
    StructField("stars", StringType(), True),
    StructField("hours", StructType([
        StructField("Monday", StringType(), True),
        StructField("Tuesday", StringType(), True),
        StructField("Wednesday", StringType(), True),
        StructField("Thursday", StringType(), True),
        StructField("Friday", StringType(), True),
        StructField("Saturday", StringType(), True),
        StructField("Sunday", StringType(), True),
    ])),
])

data = spark\
    .read\
    .schema(schema)\
    .json("yelp.json")

data.show(5, False)

# data.agg(countDistinct(col("state")).alias("count")).show()
# data.agg(countDistinct(col("name")).alias("count")).show()

data.agg(func.count(func.lit(1)).alias("city")).show()
data.agg(func.count(func.lit(1)).alias("state")).show()
