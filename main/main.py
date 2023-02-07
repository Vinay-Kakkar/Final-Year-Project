from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
import shutil
from pathlib import Path
import time


def main(spark):
    start = time.time()
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/Lines/PDBsDirectory/*")
    tempdirectory = "/Users/vinaykakkar/Desktop/PROJECT/Programs/Lines/PDBsfromRDD"

    rddkeyvalue = spark.sparkContext.wholeTextFiles(directory)

    def getNumberOfLines(k):
        os.system("wc -l "+ k[40:])
    
    rddkeyvalue.map(lambda x: getNumberOfLines(x[0])).collect()

    end = time.time()
    print(end - start)


if __name__ == '__main__':
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    main(spark)


