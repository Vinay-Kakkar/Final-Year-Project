from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
from pathlib import Path
import tempfile
import sys
import requests
import json
import urllib.parse
from os.path import exists
import sys
import re
import time
import matplotlib.pyplot as plt
from pyspark import SparkConf, SparkContext

# takes a long long time run over night


def tmalign(spark, folder1, folder2):
    # python3 main.py tmalign PDBsDirectory1 PDBsDirectory2
    directory1 = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder1+"/*")
    directory2 = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder2+"/*")

    rddkeyvalue1 = spark.wholeTextFiles(directory1)
    rddkeyvalue2 = spark.wholeTextFiles(directory2)

    rdd = rddkeyvalue1.cartesian(rddkeyvalue2)

    def runTMalign(tuple1, tuple2):
        with tempfile.NamedTemporaryFile(mode='w') as tmp1, tempfile.NamedTemporaryFile(mode='w') as tmp2:
            tmp1.write(tuple1[1])
            tmp2.write(tuple2[1])
            #print("./TMalign " + tmp1.name + " " + tmp2.name)
            os.system("./TMalign " + tmp1.name + " " + tmp2.name)

    rdd.map(lambda tuple: runTMalign(tuple[0], tuple[1])).collect()
    spark.stop()


conf1 = SparkConf().setAppName("MyApp").setMaster("local[1]") \
    .set("spark.executor.cores", "1")
spark1 = SparkContext(conf=conf1)
t1 = time.time()
tmalign(spark1, "OriginalPDBs", "OriginalPDBscopy")
t1c = time.time()


conf2 = SparkConf().setAppName("MyApp").setMaster("local[1]") \
    .set("spark.executor.cores", "8")
spark2 = SparkContext(conf=conf2)
t2 = time.time()
tmalign(spark2, "OriginalPDBs", "OriginalPDBscopy")
t2c = time.time()

conf3 = SparkConf().setAppName("MyApp").setMaster("local[2]") \
    .set("spark.executor.cores", "2")
spark3 = SparkContext(conf=conf3)
t3 = time.time()
tmalign(spark3, "OriginalPDBs", "OriginalPDBscopy")
t3c = time.time()

conf4 = SparkConf().setAppName("MyApp").setMaster("local[2]") \
    .set("spark.executor.cores", "4")
spark4 = SparkContext(conf=conf4)
t4 = time.time()
tmalign(spark4, "OriginalPDBs", "OriginalPDBscopy")
t4c = time.time()

conf5 = SparkConf().setAppName("MyApp").setMaster("local[4]") \
    .set("spark.executor.cores", "2")
spark5 = SparkContext(conf=conf5)
t5 = time.time()
tmalign(spark5, "OriginalPDBs", "OriginalPDBscopy")
t5c = time.time()

conf6 = SparkConf().setAppName("MyApp").setMaster("local[8]") \
    .set("spark.executor.cores", "1")
spark6 = SparkContext(conf=conf5)
t6 = time.time()
tmalign(spark6, "OriginalPDBs", "OriginalPDBscopy")
t6c = time.time()


times = [(t1c-t1), (t2c-t2), (t3c-t3), (t4c-t4), (t5c-t5), (t6c-t6)]
configurations = ["Config 1", "Config 2", "Config 3", "Config 4", "Config 5", "Config 6"]

plt.plot(configurations, times, linewidth=2, color="#1f77b4")
plt.ylabel("Time (seconds)", fontsize=12)
plt.xlabel("Configuration", fontsize=12)
plt.title("Line Plot of Execution Time by Configuration", fontsize=14)
plt.xticks(fontsize=10, rotation=45)
plt.yticks(fontsize=10)
plt.grid(axis="y", linestyle=":", alpha=0.7)
plt.show()