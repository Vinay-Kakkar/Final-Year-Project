from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
import shutil
from pathlib import Path
import time


def main(spark):
    start = time.time()
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/Lines/PDBsDirectory")
    tempdirectory = "/Users/vinaykakkar/Desktop/PROJECT/Programs/Lines/PDBsfromRDD"

    for file in os.listdir(os.fsencode(directory)):
        filename = os.fsdecode(file)
        if filename.endswith(".DS_Store"):
            continue
        rdd = spark.read.text("Programs/Lines/PDBsDirectory/"+filename).rdd.map(lambda x: x[0])
        if os.path.exists(tempdirectory):
            shutil.rmtree(tempdirectory)
        rdd.saveAsTextFile(tempdirectory)
        print(filename)
        os.system("wc -l "+tempdirectory+"/part-00000")
        
    end = time.time()
    print(end - start)


if __name__ == '__main__':
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    main(spark)


