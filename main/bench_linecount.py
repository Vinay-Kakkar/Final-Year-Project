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


def lineCount(spark, fileName):
    """
    Counts the number of lines in each file of a given directory using Spark.

    Args:
    - spark (SparkSession): A SparkSession object.
    - fileName (str): A string representing the name of the directory containing the files.

    Returns:
    - bool: Returns True if the function runs successfully.

    Raises:
    - Exception: If the directory specified by 'fileName' does not exist.
    - Exception: If no files are found in the specified directory.
    """
    # Function to count the number of lines in a file
    # python3 main.py linecount PDBsDirectory1
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/" + fileName + "/*")

    if not os.path.exists(fileName):
        raise Exception("Path does not exist")

    files = os.listdir(fileName)
    if not files:
        raise Exception("No files found in folder provided")
    
    # Create RDD of key-value pairs, where key is the file path and value is the file content
    rddKeyValue = spark.wholeTextFiles(directory)

    def numberOfLinesInFile(k):
        # Function to count number of lines in a file
        os.system("wc -l " + k[45:])

    # Apply the function to each key-value pair in RDD and collect the result
    rddKeyValue.map(lambda x: numberOfLinesInFile(x[0])).collect()

    spark.stop()

    return True

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
    lineCount(spark1, "OriginalPDBs")
    t1c = time.time()

    conf2 = SparkConf().setAppName("MyApp").setMaster("local[1]") \
        .set("spark.executor.cores", "8")
    spark2 = SparkContext(conf=conf2)
    t2 = time.time()
    lineCount(spark2, "OriginalPDBs")
    t2c = time.time()

    conf3 = SparkConf().setAppName("MyApp").setMaster("local[2]") \
        .set("spark.executor.cores", "2")
    spark3 = SparkContext(conf=conf3)
    t3 = time.time()
    lineCount(spark3, "OriginalPDBs")
    t3c = time.time()

    conf4 = SparkConf().setAppName("MyApp").setMaster("local[2]") \
        .set("spark.executor.cores", "4")
    spark4 = SparkContext(conf=conf4)
    t4 = time.time()
    lineCount(spark4, "OriginalPDBs")
    t4c = time.time()

    conf5 = SparkConf().setAppName("MyApp").setMaster("local[4]") \
        .set("spark.executor.cores", "2")
    spark5 = SparkContext(conf=conf5)
    t5 = time.time()
    lineCount(spark5, "OriginalPDBs")
    t5c = time.time()

    conf6 = SparkConf().setAppName("MyApp").setMaster("local[8]") \
        .set("spark.executor.cores", "1")
    spark6 = SparkContext(conf=conf6)
    t6 = time.time()
    lineCount(spark6, "OriginalPDBs")
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