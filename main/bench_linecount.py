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


def linecount(spark, filename):
    # python3 main.py linecount PDBsDirectory1
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+filename+"/*")

    if not os.path.exists(filename):
        raise Exception("Path does not exist")

    files = os.listdir(filename)
    if not files:
        raise Exception("No files found in folder provided")
    rddkeyvalue = spark.wholeTextFiles(directory)


    def numberoflinesinfile(k):
        os.system("wc -l "+ k[45:])

    rddkeyvalue.map(lambda x: numberoflinesinfile(x[0])).collect()

    spark.stop()

    return True


conf1 = SparkConf().setAppName("MyApp").setMaster("local[1]") \
    .set("spark.executor.cores", "1")
spark1 = SparkContext(conf=conf1)
t1 = time.time()
linecount(spark1, "OriginalPDBs")
t1c = time.time()


conf2 = SparkConf().setAppName("MyApp").setMaster("local[1]") \
    .set("spark.executor.cores", "8")
spark2 = SparkContext(conf=conf2)
t2 = time.time()
linecount(spark2, "OriginalPDBs")
t2c = time.time()

conf3 = SparkConf().setAppName("MyApp").setMaster("local[2]") \
    .set("spark.executor.cores", "2")
spark3 = SparkContext(conf=conf3)
t3 = time.time()
linecount(spark3, "OriginalPDBs")
t3c = time.time()

conf4 = SparkConf().setAppName("MyApp").setMaster("local[2]") \
    .set("spark.executor.cores", "4")
spark4 = SparkContext(conf=conf4)
t4 = time.time()
linecount(spark4, "OriginalPDBs")
t4c = time.time()

conf5 = SparkConf().setAppName("MyApp").setMaster("local[4]") \
    .set("spark.executor.cores", "2")
spark5 = SparkContext(conf=conf5)
t5 = time.time()
linecount(spark5, "OriginalPDBs")
t5c = time.time()

conf6 = SparkConf().setAppName("MyApp").setMaster("local[8]") \
    .set("spark.executor.cores", "1")
spark6 = SparkContext(conf=conf5)
t6 = time.time()
linecount(spark6, "OriginalPDBs")
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