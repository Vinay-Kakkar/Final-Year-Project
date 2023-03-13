from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
import shutil
from pathlib import Path
import time
import tempfile

# Need to run this file in the correct location in the terminal to work
def main(spark):
    start = time.time()
    directory1 = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/TMalign/PDBsDirectory1/*")
    directory2 = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/TMalign/PDBsDirectory2/*")

    rddkeyvalue1 = spark.sparkContext.wholeTextFiles(directory1)
    rddkeyvalue2 = spark.sparkContext.wholeTextFiles(directory2)

    rdd = rddkeyvalue1.cartesian(rddkeyvalue2)

    def runTMalign(tuple1, tuple2):
        with tempfile.NamedTemporaryFile(prefix = tuple1[0][72:], mode='w') as tmp1, tempfile.NamedTemporaryFile(prefix = tuple2[0][72:], mode='w') as tmp2:
            tmp1.write(tuple1[1])
            tmp2.write(tuple2[1])
            #print("./TMalign " + tmp1.name + " " + tmp2.name)
            os.system("./TMalign " + tmp1.name + " " + tmp2.name)

    rdd.map(lambda tuple: runTMalign(tuple[0], tuple[1])).collect()
        
    end = time.time()
    print(end - start)


if __name__ == '__main__':
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    main(spark)


# If in the case we can not hold files in the hdfs we will need to cartesian each the rdd values then have the function create the file then run the tmalign function