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


def linecount(spark, fileName):
    """
    Returns the line count for each file in the directory given by 'fileName'.
    The function takes a SparkSession object and a string 'fileName' as input.

    Args:
    - spark (SparkSession): A SparkSession object
    - fileName (str): A string representing the name of the directory to count lines in

    Returns:
    - True: A boolean indicating that the function has completed executing successfully.

    Raises:
    - Exception: If 'fileName' does not exist or if no files are found in the folder provided
    """
    # python3 main.py linecount PDBsDirectory1
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+fileName+"/*")

    if not os.path.exists(fileName):
        raise Exception("Path does not exist")

    files = os.listdir(fileName)
    if not files:
        raise Exception("No files found in folder provided")
    rddKeyValue = spark.sparkContext.wholeTextFiles(directory)


    def numberoflinesinfile(k):
        os.system("wc -l "+ k[45:])

    rddKeyValue.map(lambda x: numberoflinesinfile(x[0])).collect()

    return True

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

def searchpdbfiles(value):
    """
    Searches for PDB files containing the search value provided.
    The function takes a string 'value' as input.

    Args:
    - value (str): A string representing the search value to look for in PDB files.

    Returns:
    - listOfResults (list): A list of dictionaries representing the search results.

    Raises:
    - Exception: If the input string contains invalid characters or if an API error occurs.
    """
    # python3 main.py searchpdbfiles vinay
    if bool(re.search('[!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]', value)):
        raise Exception("Invalid Input please dont use punctuations")
    jsonfile = 'main/Search.json'
    try:
        with open(file=jsonfile, mode="r") as jsonFile:
            data = json.load(jsonFile)
    except:
        raise Exception("Invalid json")
    data['query']['parameters']['value'] = value
    with open(file=jsonfile, mode="w") as jsonFile:
        json.dump(data, jsonFile)
    #fix to make the values in data not use ' qoutes but use "" qoutes instead
    data = json.dumps(data)
    newData = urllib.parse.quote(data)
    apiCall = 'https://search.rcsb.org/rcsbsearch/v2/query?json={}'.format(newData)
    #Check this line to see what response you are getting if code stops working
    result = requests.get(apiCall)
    if result.status_code != 200:
        raise Exception("API error occurred")
    resultJson = result.json()
    listOfResults = []
    for result in resultJson["result_set"]:
        listOfResults.append(result)
    print(listOfResults)
    return listOfResults
    #503 Service Unavailable. The server is currently unable to handle the request due to a temporary overloading

def getpdbfiles(folder, value):
    """
    Downloads PDB files containing the search value provided to the directory 'folder'.
    The function takes two strings 'folder' and 'value' as input.

    Args:
    - folder (str): A string representing the name of the directory to download PDB files to.
    - value (str): A string representing the search value to look for in PDB files.

    Returns:
    - None

    Raises:
    - Exception: If the input string contains invalid characters or if an API error occurs.
    """
    # python3 main.py getpdbfiles PDBsDirectory1 vinay
    if bool(re.search('[!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]', value)):
        raise Exception("Invalid Input please dont use punctuations")
    jsonfile = 'Search.json'
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder+"/")
    try:
        with open(file=jsonfile, mode="r") as jsonFile:
            data = json.load(jsonFile)
    except:
        raise Exception("Invalid json")
    data['query']['parameters']['value'] = value


    with open(file=jsonfile, mode="w") as jsonFile:
        json.dump(data, jsonFile)

    #fix to make the values in data not use ' qoutes but use "" qoutes instead
    data = json.dumps(data)

    newData = urllib.parse.quote(data)
    apiCall = 'https://search.rcsb.org/rcsbsearch/v2/query?json={}'.format(newData)


    #Check this line to see what response you are getting if code stops working
    result = requests.get(apiCall)
    if result.status_code != 200:
        raise Exception("API error occurred")
    result = result.json()

    listOfResults = []
    for x in result["result_set"]:
        listOfResults.append(x['identifier'])
    #Eveything above is for getting the values of the search

    for pdbFile in listOfResults:
        if exists(directory + pdbFile + '.pdb'):
            print(pdbFile + ': already exists')
            continue
        apiCall = 'https://files.rcsb.org/download/{}.pdb'.format(pdbFile)

        try:
            response = requests.get(apiCall)
        except:
            raise Exception("API error occurred")

        with open(directory + pdbFile + '.pdb', 'wb') as f:
            f.write(response.content)

    #503 Service Unavailable. The server is currently unable to handle the request due to a temporary overloading

def getcurrentpdbfiles(folderPath):
    """
    Returns a list of absolute paths for all PDB files in the specified directory and its subdirectories.

    Parameters:
    folder_path (str): The path of the directory to search for PDB files.

    Returns:
    list: A list of absolute paths for all PDB files found in the specified directory and its subdirectories.
    """
    # python3 main.py getcurrentpdbfiles PDBsDirectory1
    listOfPdbs = []
    if os.path.exists(folderPath) == False:
        raise Exception("Invalid folder path")
    for root, dirs, files in os.walk(folderPath):
        for file in files:
            if file.endswith('.pdb'):
                listOfPdbs.append(os.path.join(root, file))
    return listOfPdbs

def emptypdbfolder(folderPath):
    """
    Deletes all files within the specified directory and its subdirectories.

    Parameters:
    folder_path (str): The path of the directory to empty.

    Returns:
    None
    """
    # python3 main.py emptypdbfolder PDBsDirectory1
    # iterate through all files and subdirectories in the provided folder
    print(folderPath)
    for root, dirs, files in os.walk(folderPath):
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

