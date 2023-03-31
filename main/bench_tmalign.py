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
    """
    Performs TM-align structural alignment of proteins in the directories 'folder1' and 'folder2'.
    
    Args:
        spark (SparkSession): A SparkSession object.
        folder1 (str): A string representing the path of the first directory containing protein PDB files.
        folder2 (str): A string representing the path of the second directory containing protein PDB files.

    Returns:
        None

    Raises:
        Exception: If the path for either of the folders doesn't exist.
        Exception: If there are no PDB files found in the directories.
    """
    # python3 main.py tmalign PDBsDirectory1 PDBsDirectory2
    if not os.path.exists(folder1):
        raise Exception("Path does not exist")
    if not os.path.exists(folder2):
        raise Exception("Path does not exist")
    
    directory1 = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder1+"/*")
    directory2 = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder2+"/*")

    
    files = os.listdir(folder1)
    if not files:
        raise Exception("No files found in folder provided")
    files = os.listdir(folder1)
    if not files:
        raise Exception("No files found in folder provided")

    rddKeyValue1 = spark.wholeTextFiles(directory1)
    rddKeyValue2 = spark.wholeTextFiles(directory2)

    rdd = rddKeyValue1.cartesian(rddKeyValue2)

    def runTMalign(tuple1, tuple2):
        with tempfile.NamedTemporaryFile( mode='w') as tmp1, tempfile.NamedTemporaryFile( mode='w') as tmp2:
            tmp1.write(tuple1[1])
            tmp2.write(tuple2[1])
            #print("./TMalign " + tmp1.name + " " + tmp2.name)
            os.system("./TMalign " + tmp1.name + " " + tmp2.name)

    rdd.map(lambda tuple: runTMalign(tuple[0], tuple[1])).collect()
    spark.stop()

def bench():
    """
    Executes the lineCount function with different Spark configurations and plots a graph of execution times.

    Returns:
        None
    
    Raises:
        None    

    Output:
    - A graph of execution times by configuration.
    """
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


    # Create a list of (configuration name, execution time) tuples
    times = [("Config 1", t1c - t1), ("Config 2", t2c - t2), ("Config 3", t3c - t3),
             ("Config 4", t4c - t4), ("Config 5", t5c - t5), ("Config 6", t6c - t6)]

    # Extract the configuration names and their corresponding execution times from the tuples
    configurations = [name for name, _ in times]
    execution_times = [time for _, time in times]

    # Plot the bar chart
    plt.bar(configurations, execution_times, color="#1f77b4")

    # Add text labels for each bar
    for i, time1 in enumerate(execution_times):
        plt.text(i, time1 + 1, "{:.2f}s".format(time1), ha="center", fontsize=10)

    # Set the chart title, axis labels, and customize other settings
    plt.ylim(0, max(execution_times)+(max(execution_times)//2))
    plt.ylabel("Time (seconds)", fontsize=12)
    plt.xlabel("Configuration", fontsize=12)
    plt.title("Line Plot of Execution Time by Configuration", fontsize=14)
    plt.xticks(fontsize=10, rotation=45)
    plt.yticks(fontsize=10)
    plt.grid(axis="y", linestyle=":", alpha=0.7)

    # Show the chart
    plt.show()
bench()