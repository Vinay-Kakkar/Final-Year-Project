from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
import shutil
from pathlib import Path
import time
import tempfile




def main(spark):



    start = time.time()
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/Programs/Lines/PDBsDirectory/*")
    tempdirectory = "/Users/vinaykakkar/Desktop/PROJECT/Programs/Lines/PDBsfromRDD"

    rddkeyvalue = spark.sparkContext.wholeTextFiles(directory)

    def numberoflinesinfile(filename, filecontent):
        # Can potentially use dir parmeter for a better output https://docs.python.org/3/library/tempfile.html#tempfile.NamedTemporaryFile
        with tempfile.NamedTemporaryFile(prefix=filename[69:], mode='w') as tmp:
            tmp.write(filecontent)
            os.system("wc -l "+ tmp.name)
    
    rddkeyvalue.map(lambda x: numberoflinesinfile(x[0], x[1])).collect()

    end = time.time()
    print(end - start)


if __name__ == '__main__':
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    main(spark)