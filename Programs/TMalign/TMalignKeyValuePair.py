from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
import shutil
from pathlib import Path
import time

# Need to run this file in the correct location in the terminal to work
def main(spark):
    start = time.time()
    directory1 = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/TMalign/PDBsDirectory1/*")
    directory2 = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/TMalign/PDBsDirectory2/*")

    rddkeyvalue1 = spark.sparkContext.wholeTextFiles(directory1)
    rddkeyvalue2 = spark.sparkContext.wholeTextFiles(directory2)

    keys1 = rddkeyvalue1.keys()
    keys2 = rddkeyvalue2.keys()

    rdd = keys1.cartesian(keys2)

    def runTMalign(tuple):
        print("./TMalign " + tuple[0][57:] + " " + tuple[1][57:])
        os.system("./TMalign " + tuple[0][57:] + " " + tuple[1][57:])

    rdd.map(lambda tuple: runTMalign(tuple)).collect()
        
    end = time.time()
    print(end - start)


if __name__ == '__main__':
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    main(spark)


# If in the case we can not hold files in the hdfs we will need to cartesian each the rdd values then have the function create the file then run the tmalign function