from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
import shutil
from pathlib import Path
import time


def main(spark):
    start = time.time()
    directory1 = ("/Users/vinaykakkar/Desktop/PROJECT/main/PDBsDirectory1")
    directory2 = ("/Users/vinaykakkar/Desktop/PROJECT/main/PDBsDirectory2")

    rddkeyvalue1 = spark.sparkContext.wholeTextFiles(directory1)
    rddkeyvalue2 = spark.sparkContext.wholeTextFiles(directory1)

    def numberoflinesinfile(k):
        os.system("wc -l "+ k[40:])
    
    rddkeyvalue1.map(lambda x: numberoflinesinfile(x[0])).collect()

    end = time.time()
    print(end - start)


if __name__ == '__main__':
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    main(spark)


