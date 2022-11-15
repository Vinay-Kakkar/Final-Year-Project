from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import os

spark = SparkSession.builder.appName('PDB').getOrCreate()

pdb = spark.read.option('header','true').csv('Proof of Concepts/PDB onto a Cluster/AF-Q5VSL9-F1-model_v4.pdb', inferSchema = True)

rdd = pdb.rdd.flatMap(lambda x: list(x))

df = spark.createDataFrame(rdd, StringType())

f = open("Proof of Concepts/PDB onto a Cluster/PDBs for Executables/newPDB.pdb", 'w')

for i in df.collect():
    f.write(i[0]+"\n")
f.close()

os.system("wc -l Proof\ of\ Concepts/PDB\ onto\ a\ Cluster/PDBs\ for\ Executables/newPDB.pdb")


