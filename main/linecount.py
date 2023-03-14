from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
import shutil
from pathlib import Path
import time
import sys


def main(filename):
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    start = time.time()
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+filename+"/*")

    rddkeyvalue = spark.sparkContext.wholeTextFiles(directory)

    def numberoflinesinfile(k):
        os.system("wc -l "+ k[45:])
    
    rddkeyvalue.map(lambda x: numberoflinesinfile(x[0])).collect()

    end = time.time()
    print(end - start)


if __name__ == '__main__':
    globals()[sys.argv[1]](sys.argv[2])

#Example run line: python3 linecount.py main PDBsDirectory2