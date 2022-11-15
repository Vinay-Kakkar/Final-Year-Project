from pdb import Pdb
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('PDB').getOrCreate()

pdb = spark.read.option('header','true').csv('Proof of Concepts/AF-A0A452S449-F1-model_v3.pdb', inferSchema = True)

pdb.show()
#print("Type of datafram is: " , type(pdb))
#print("Columns in dataframe: " , pdb.columns)
#print("first 2 values in dataframe: " , pdb.head(1))

for row in pdb.collect():
    for value in row:
        print(value)


rdd=pdb.rdd.map(lambda x: 
    (print(x[0]))
    ) 

df2=rdd.toDF(["Header"])
df2.show()
#Pdb.rdd.filter(lambda r: str(r['target']).startswith('good'))

#pdb_rdd = pdb.rdd.filter(lambda x: 'ATOM').take(5)

#print(pdb_rdd)
