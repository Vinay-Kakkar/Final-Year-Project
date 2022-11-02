from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Dataframe').getOrCreate()

df_pyspark = spark.read.option('header','true').csv('test1.csv', inferSchema = True)

df_pyspark.printSchema()

print("Type of datafram is: " , type(df_pyspark))
print("Columns in dataframe: " , df_pyspark.columns)
print("first 2 values in dataframe: " , df_pyspark.head(2))
df_pyspark.select('name').show()
df_pyspark.select(['name' , 'age']).show()

df_pyspark.describe().show()

##Does not change the values in the csv file
df_pyspark.withColumn('age after one year', df_pyspark['age']+1).show()
df_pyspark.drop('age after one year').show()
df_pyspark.withColumnRenamed('name','Name').show()