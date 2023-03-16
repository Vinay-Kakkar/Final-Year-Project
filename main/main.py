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


def linecount(spark, filename):
    # python3 main.py linecount PDBsDirectory1
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+filename+"/*")

    if not os.path.exists(filename):
        raise Exception("Path does not exist")

    files = os.listdir(filename)
    if not files:
        raise Exception("No files found in folder provided")
    rddkeyvalue = spark.sparkContext.wholeTextFiles(directory)


    def numberoflinesinfile(k):
        os.system("wc -l "+ k[45:])

    rddkeyvalue.map(lambda x: numberoflinesinfile(x[0])).collect()

    return True

def tmalign(spark, folder1, folder2):
    # python3 main.py tmalign PDBsDirectory1 PDBsDirectory2
    directory1 = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder1+"/*")
    directory2 = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder2+"/*")

    rddkeyvalue1 = spark.sparkContext.wholeTextFiles(directory1)
    rddkeyvalue2 = spark.sparkContext.wholeTextFiles(directory2)

    rdd = rddkeyvalue1.cartesian(rddkeyvalue2)

    def runTMalign(tuple1, tuple2):
        with tempfile.NamedTemporaryFile(prefix = tuple1[0][60:], mode='w') as tmp1, tempfile.NamedTemporaryFile(prefix = tuple2[0][60:], mode='w') as tmp2:
            tmp1.write(tuple1[1])
            tmp2.write(tuple2[1])
            #print("./TMalign " + tmp1.name + " " + tmp2.name)
            os.system("./TMalign " + tmp1.name + " " + tmp2.name)

    rdd.map(lambda tuple: runTMalign(tuple[0], tuple[1])).collect()

def searchpdbfiles(value):
    # python3 main.py searchpdbfiles vinay
    jsonfile = 'Search.json'


    with open(file=jsonfile, mode="r") as jsonFile:
        data = json.load(jsonFile)

    data['query']['parameters']['value'] = value


    with open(file=jsonfile, mode="w") as jsonFile:
        json.dump(data, jsonFile)

    #fix to make the values in data not use ' qoutes but use "" qoutes instead
    data = json.dumps(data)

    newdata = urllib.parse.quote(data)
    apicall = 'https://search.rcsb.org/rcsbsearch/v2/query?json={}'.format(newdata)


    #Check this line to see what response you are getting if code stops working
    result = requests.get(apicall)
    result = result.json()

    for x in result["result_set"]:
        print(x)
    #503 Service Unavailable. The server is currently unable to handle the request due to a temporary overloading

def getpdbfiles(folder, value):
    # python3 main.py getpdbfiles PDBsDirectory1 vinay
    jsonfile = 'Search.json'
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder+"/")


    with open(file=jsonfile, mode="r") as jsonFile:
        data = json.load(jsonFile)

    data['query']['parameters']['value'] = value


    with open(file=jsonfile, mode="w") as jsonFile:
        json.dump(data, jsonFile)

    #fix to make the values in data not use ' qoutes but use "" qoutes instead
    data = json.dumps(data)

    newdata = urllib.parse.quote(data)
    apicall = 'https://search.rcsb.org/rcsbsearch/v2/query?json={}'.format(newdata)


    #Check this line to see what response you are getting if code stops working
    result = requests.get(apicall)
    result = result.json()

    listofresults = []
    for x in result["result_set"]:
        listofresults.append(x['identifier'])
    #Eveything above is for getting the values of the search

    for pdbfile in listofresults:
        if exists(directory + pdbfile + '.pdb'):
            print(pdbfile + ': already exists')
            continue
        apicall = 'https://files.rcsb.org/download/{}.pdb'.format(pdbfile)

        response = requests.get(apicall)

        with open(directory + pdbfile + '.pdb', 'wb') as f:
            f.write(response.content)

    #503 Service Unavailable. The server is currently unable to handle the request due to a temporary overloading

def getcurrentpdbfiles(folderpath):
    # python3 main.py getcurrentpdbfiles PDBsDirectory1
    lisofpdbs = []
    if os.path.exists(folderpath) == False:
        raise Exception("Invalid folder path")
    for root, dirs, files in os.walk(folderpath):
        for file in files:
            if file.endswith('.pdb'):
                lisofpdbs.append(os.path.join(root, file))
    return lisofpdbs

def emptypdbfolder(folder_path):
    # python3 main.py emptypdbfolder PDBsDirectory1
    # iterate through all files and subdirectories in the provided folder
    print(folder_path)
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            # delete the file
            os.remove(os.path.join(root, file))

if __name__ == '__main__':
    if sys.argv[1] == "linecount":
        spark = SparkSession.builder.appName('PDB').getOrCreate()
        globals()[sys.argv[1]](spark,sys.argv[2])
    elif sys.argv[1] == "tmalign":
        spark = SparkSession.builder.appName('PDB').getOrCreate()
        globals()[sys.argv[1]](spark, sys.argv[2],sys.argv[3])
    elif sys.argv[1] == "searchpdbfiles":
        globals()[sys.argv[1]](sys.argv[2])
    elif sys.argv[1] == "getpdbfiles":
        globals()[sys.argv[1]](sys.argv[2],sys.argv[3])
    elif sys.argv[1] == "getcurrentpdbfiles":
        globals()[sys.argv[1]](sys.argv[2])
    elif sys.argv[1] == "emptypdbfolder":
        globals()[sys.argv[1]](sys.argv[2])
    else:
        print("Un recognised function call please check readme for valid function calls")

