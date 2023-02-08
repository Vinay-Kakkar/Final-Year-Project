from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
import shutil
from pathlib import Path
import time


def main(spark):
    start = time.time()
    directory1 = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/TMalign/PDBsDirectory1/*")
    directory2 = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/TMalign/PDBsDirectory2/*")

    rddkeyvalue1 = spark.sparkContext.wholeTextFiles(directory1)
    rddkeyvalue2 = spark.sparkContext.wholeTextFiles(directory2)

    lis = []

    def getNextKey(y):
        rdd.map(lambda x: runTmAlign(x[0], y)).collect()


    def runTmAlign(x, y):
        os.system("./TMalign " + x + " " + y)

    rdd = rddkeyvalue1.union(rddkeyvalue2).groupByKey()

    rdd.map(lambda x: getNextKey(x[0])).collect()


    print(lis)
        
    end = time.time()
    print(end - start)


if __name__ == '__main__':
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    main(spark)


