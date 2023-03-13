from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
import shutil
from pathlib import Path
import time


def main(spark):
    start = time.time()
    directory1 = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/TMalign/PDBsDirectory1")
    directory2 = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/TMalign/PDBsDirectory2")
    tempdirectory1 = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/TMalign/PDBsfromRDD1")
    tempdirectory2 = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/TMalign/PDBsfromRDD2")

    for file in os.listdir(os.fsencode(directory1)):
        filename = os.fsdecode(file)
        if filename.endswith(".DS_Store"):
            continue
        rdd = spark.read.text("PDBsDirectory1/"+filename).rdd.map(lambda x: x[0])
        if os.path.exists(tempdirectory1):
            shutil.rmtree(tempdirectory1)
        rdd.saveAsTextFile(tempdirectory1)

        for file in os.listdir(os.fsencode(directory2)):
            filename = os.fsdecode(file)
            if filename.endswith(".DS_Store"):
                continue
            rdd = spark.read.text("PDBsDirectory2/"+filename).rdd.map(lambda x: x[0])
            if os.path.exists(tempdirectory2):
                shutil.rmtree(tempdirectory2)
            rdd.saveAsTextFile(tempdirectory2)
            os.system("./TMalign PDBsfromRDD1/part-00000 PDBsfromRDD2/part-00000")
        
    end = time.time()
    print(end - start)


if __name__ == '__main__':
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    main(spark)


