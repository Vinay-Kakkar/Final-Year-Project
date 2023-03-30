from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
import shutil
from pathlib import Path
import time
import tempfile
import sys

# Need to run this file in the correct location in the terminal to work
def main(folder1, folder2):
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    start = time.time()
    directory1 = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder1+"/*")
    directory2 = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder2+"/*")

    rddKeyValue1 = spark.sparkContext.wholeTextFiles(directory1)
    rddKeyValue2 = spark.sparkContext.wholeTextFiles(directory2)

    rdd = rddKeyValue1.cartesian(rddKeyValue2)

    def runTMalign(tuple1, tuple2):
        with tempfile.NamedTemporaryFile(prefix = tuple1[0][60:], mode='w') as tmp1, tempfile.NamedTemporaryFile(prefix = tuple2[0][60:], mode='w') as tmp2:
            tmp1.write(tuple1[1])
            tmp2.write(tuple2[1])
            #print("./TMalign " + tmp1.name + " " + tmp2.name)
            os.system("./TMalign " + tmp1.name + " " + tmp2.name)

    rdd.map(lambda tuple: runTMalign(tuple[0], tuple[1])).collect()
        
    end = time.time()
    print(end - start)


if __name__ == '__main__':
    globals()[sys.argv[1]](sys.argv[2],sys.argv[3])

#Example run line: python3 tmalign.py main PDBsDirectory1 PDBsDirectory2 


# If in the case we can not hold files in the hdfs we will need to cartesian each the rdd values then have the function create the file then run the tmalign function