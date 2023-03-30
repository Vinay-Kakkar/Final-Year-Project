import pdb
import shutil
from tempfile import TemporaryDirectory
import unittest
import main
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
import pandas as pd
import os
import re
import requests
import json
import urllib
from os.path import exists

#Created temp function due to the issues of working directories test runs the code from /PROJECT where main is build to be run from PROJECT/main
def templinecount(spark, fileName):
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+fileName+"/*")

    if not os.path.exists("main/"+fileName):
        raise Exception("Path does not exist")

    files = os.listdir("main/"+fileName)
    if not files:
        raise Exception("No files found in folder provided")
    rddKeyValue = spark.sparkContext.wholeTextFiles(directory)
    def numberoflinesinfile(k):
        os.system("wc -l "+ k[45:])
    rddKeyValue.map(lambda x: numberoflinesinfile(x[0])).collect()
    return True

def tempsearchpdbfiles(value, jsonPath, api):
    # python3 main.py searchpdbfiles vinay
    if bool(re.search('[!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]', value)):
        raise Exception("Invalid Input please dont use punctuations")
    jsonfile = jsonPath
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
    apiCall = (api+'json={}'.format(newData))
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

def tempgetpdbfiles(folder, value, jsonPath, api):
    if bool(re.search('[!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]', value)):
        raise Exception("Invalid Input please dont use punctuations")
    jsonfile = jsonPath
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder+"/")
    try:
        with open(file=jsonfile, mode="r") as jsonFile:
            data = json.load(jsonFile)
    except:
        raise Exception("Invalid json")
    data['query']['parameters']['value'] = value
    with open(file=jsonfile, mode="w") as jsonFile:
        json.dump(data, jsonFile)
    data = json.dumps(data)
    newData = urllib.parse.quote(data)
    apiCall = 'https://search.rcsb.org/rcsbsearch/v2/query?json={}'.format(newData)
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
        apiCall = api+'/{}.pdb'.format(pdbFile)
        try:
            response = requests.get(apiCall)
        except:
            raise Exception("API error occurred")
        with open(directory + pdbFile + '.pdb', 'wb') as f:
            f.write(response.content)

class Testgetcurrentpdbfiles(unittest.TestCase):
    directory = ("main/testdirectory")

    #Test that the function returns a list of file names when given a valid directory path.
    def test_checkreturntype(self):
        os.mkdir(self.directory)
        open(os.path.join(self.directory, "fake.pdb"), "w").close()
        test = main.getcurrentpdbfiles(self.directory)
        self.assertTrue(type(test) is list)

        os.remove(os.path.join(self.directory, "fake.pdb"))
        os.rmdir(self.directory)
    
    #Test that the function returns an empty list when given a directory path that contains no files.
    def test_checkemptyfolder(self):
        os.mkdir(self.directory)
        test = main.getcurrentpdbfiles(self.directory)
        self.assertTrue(type(test) is list)
        os.rmdir(self.directory)
    
    #Test that the function raises an exception when given an invalid directory path (e.g., a path that does not exist).
    def test_invalidpath(self):
        with self.assertRaises(Exception) as context:
            main.getcurrentpdbfiles(self.directory)
        self.assertTrue("Invalid folder path" in str(context.exception))

    #Test that the function correctly handles nested directories and returns all files within them.
    def test_subdir(self):
        subDirectory = "subfolder"
        os.mkdir(self.directory)
        open(os.path.join(self.directory, "fake1.pdb"), "w").close()
        os.mkdir(os.path.join(self.directory, subDirectory))
        open(os.path.join(self.directory, subDirectory, "fake2.pdb"), "w").close()


        test = main.getcurrentpdbfiles(self.directory)
        self.assertTrue(len(test) is 2)

        os.remove(os.path.join(self.directory, "fake1.pdb"))
        os.remove(os.path.join(self.directory, subDirectory, "fake2.pdb"))
        os.rmdir(os.path.join(self.directory, subDirectory))
        os.rmdir(self.directory)
    
    #Test that the function only returns files and not directories or other types of files (e.g., symbolic links).
    def test_subdir(self):
        subDirectory = "subfolder"
        os.mkdir(self.directory)
        open(os.path.join(self.directory, "fake1.pdb"), "w").close()
        os.mkdir(os.path.join(self.directory, subDirectory))
        open(os.path.join(self.directory, subDirectory, "fake2.pdb"), "w").close()

        os.symlink(os.path.join(self.directory, "file1.txt"), os.path.join(self.directory, "link1"))
        os.symlink(os.path.join(self.directory, "subdirectory"), os.path.join(self.directory, "link2"))


        test = main.getcurrentpdbfiles(self.directory)
        self.assertTrue(len(test) is 2)

        os.remove(os.path.join(self.directory, "fake1.pdb"))
        os.remove(os.path.join(self.directory, "link1"))
        os.remove(os.path.join(self.directory, "link2"))
        os.remove(os.path.join(self.directory, subDirectory, "fake2.pdb"))
        os.rmdir(os.path.join(self.directory, subDirectory))
        os.rmdir(self.directory)
   
class Testlinecount(unittest.TestCase):
    directory = ("main/testdirectory")
    directoryWithOutMain = ("testdirectory")

    #Test that the function returns Exception when there is no files in folder
    def test_empty_folder_path(self):
        with self.assertRaises(Exception) as context:
            spark = SparkSession.builder.appName('Test').getOrCreate()
            os.mkdir(self.directory)
            test = templinecount(spark, self.directoryWithOutMain)
        self.assertTrue("No files found in folder provided" in str(context.exception))
        
        os.rmdir(self.directory)

    #Test that the function returns true when given a folder with a file:
    def test_folder_with_file(self):
        spark = SparkSession.builder.appName('Test').getOrCreate()
        os.mkdir(self.directory)
        open(os.path.join(self.directory, "fake.pdb"), "w").close()
        test = templinecount(spark, self.directoryWithOutMain)
        self.assertTrue(test is True)
        
        os.remove(os.path.join(self.directory, "fake.pdb"))
        os.rmdir(self.directory)

    #Test that the function returns true when given a folder with multiple files:
    def test_folder_with_multiple_file(self):

        spark = SparkSession.builder.appName('Test').getOrCreate()

        subDirectory = "subfolder"
        os.mkdir(self.directory)
        open(os.path.join(self.directory, "fake1.pdb"), "w").close()
        os.mkdir(os.path.join(self.directory, subDirectory))
        open(os.path.join(self.directory, subDirectory, "fake2.pdb"), "w").close()

        test = templinecount(spark, self.directoryWithOutMain)
        self.assertTrue(test is True)
        
        os.remove(os.path.join(self.directory, "fake1.pdb"))
        os.remove(os.path.join(self.directory, subDirectory, "fake2.pdb"))
        os.rmdir(os.path.join(self.directory, subDirectory))
        os.rmdir(self.directory)
    
    #Test that the function returns exception when there is a invalid folder
    def test_invalid_folder(self):
        spark = SparkSession.builder.appName('Test').getOrCreate()
        with self.assertRaises(Exception) as context:
            test = templinecount(spark, self.directoryWithOutMain)
        self.assertTrue("Path does not exist" in str(context.exception))
        
class Testsearchpdbfiles(unittest.TestCase):
    directory = ("main/testdirectory")

    #Test that the function returns the correct result when passed a valid input value.
    def test_validinput(self):
        value = "covid"
        test = tempsearchpdbfiles(value, 'main/Search.json', 'https://search.rcsb.org/rcsbsearch/v2/query?')
        result = {'identifier': '7N0R', 'score': 1.0}
        self.assertTrue(type(test) is list)
        self.assertTrue(test[0] == result)

    #Test that the function raises an exception when passed an invalid input value (e.g. a string instead of an integer).
    def test_invalidinput(self):
        value = "___"
        with self.assertRaises(Exception) as context:
            test = tempsearchpdbfiles(value, 'main/Search.json', 'https://search.rcsb.org/rcsbsearch/v2/query?')
        self.assertTrue("Invalid Input please dont use punctuations" in str(context.exception))

    #Test that the function correctly handles errors returned by the API.
    def test_apiexception(self):
        value = "covid"
        with self.assertRaises(Exception) as context:
            test = tempsearchpdbfiles(value, 'main/Search.json', 'https://search.rcsb.org/rcsbsearc')
        self.assertTrue("API error occurred" in str(context.exception))

    #Test that the function returns an error if the JSON file cannot be read.
    def test_invalidjson(self):
        value = "covid"
        with self.assertRaises(Exception) as context:
            test = tempsearchpdbfiles(value, 'main/invalid.json', 'https://search.rcsb.org/rcsbsearch/v2/query?')
        self.assertTrue("Invalid json" in str(context.exception))

class Testgetpdbfiles(unittest.TestCase):
    directory = ("main/testdirectory")
    directoryWithOutMain = ("testdirectory")
    #Test that the function raises an error when an invalid input is passed.
    def test_invalidinput(self):
        os.mkdir(self.directory)
        value = "___"
        with self.assertRaises(Exception) as context:
            test = tempgetpdbfiles(self.directoryWithOutMain, value, 'main/Search.json', 'https://search.rcsb.org/rcsbsearch/v2/query?')
        self.assertTrue("Invalid Input please dont use punctuations" in str(context.exception))

        os.rmdir(self.directory)
    
    #Test that the function successfully reads and loads the JSON file.
    def test_valid_json(self):
        os.mkdir(self.directory)
        value = "covid"
        result = tempgetpdbfiles(self.directoryWithOutMain, value, 'main/Search.json', 'https://search.rcsb.org/rcsbsearch/v2/query?')
        self.assertIsNone(result)
        for fileName in os.listdir(self.directory):
            filePath = os.path.join(self.directory, fileName)
            try:
                if os.path.isfile(filePath) or os.path.islink(filePath):
                    os.unlink(filePath)
                elif os.path.isdir(filePath):
                    shutil.rmtree(filePath)
            except Exception as e:
                print(f'Failed to delete {filePath}. Reason: {e}')
        os.rmdir(self.directory)
    
    
    #Test that the function successfully receives a response from the API.
    def test_api_response(self):
        os.mkdir(self.directory)
        value = "covid"
        apiUrl='https://files.rcsb.org/download'
        response = tempgetpdbfiles(self.directoryWithOutMain, value, 'main/Search.json', apiUrl)
        self.assertIsNone(response)
        for fileName in os.listdir(self.directory):
            filePath = os.path.join(self.directory, fileName)
            try:
                if os.path.isfile(filePath) or os.path.islink(filePath):
                    os.unlink(filePath)
                elif os.path.isdir(filePath):
                    shutil.rmtree(filePath)
            except Exception as e:
                print(f'Failed to delete {filePath}. Reason: {e}')
        os.rmdir(self.directory)
    
    #Test that the function correctly handles any errors or exceptions that may occur during the API call (e.g. network errors, server errors).
    def test_api_call_error_handling(self):
        os.mkdir(self.directory)
        value = "covid"
        apiUrl='https://files.rcsb.'
        with self.assertRaises(Exception) as context:
            tempgetpdbfiles(self.directoryWithOutMain, value, 'main/Search.json', apiUrl)
        self.assertTrue("API error occurred" in str(context.exception))
        for fileName in os.listdir(self.directory):
            filePath = os.path.join(self.directory, fileName)
            try:
                if os.path.isfile(filePath) or os.path.islink(filePath):
                    os.unlink(filePath)
                elif os.path.isdir(filePath):
                    shutil.rmtree(filePath)
            except Exception as e:
                print(f'Failed to delete {filePath}. Reason: {e}')
        os.rmdir(self.directory)

    #Test that the function correctly parses and returns the data from the API response.
    def test_pdb_content(self):
        os.mkdir(self.directory)
        value = "covid"
        apiUrl='https://files.rcsb.org/download'
        result = tempgetpdbfiles(self.directoryWithOutMain, value, 'main/Search.json', apiUrl)
        self.assertIsNone(result)
        filePath = os.path.join(self.directory, '6YUN.pdb')
        with open(filePath, 'r') as f:
            content = f.read()
        self.assertTrue(content.startswith('HEADER'))
        for fileName in os.listdir(self.directory):
            filePath = os.path.join(self.directory, fileName)
            try:
                if os.path.isfile(filePath) or os.path.islink(filePath):
                    os.unlink(filePath)
                elif os.path.isdir(filePath):
                    shutil.rmtree(filePath)
            except Exception as e:
                print(f'Failed to delete {filePath}. Reason: {e}')
        os.rmdir(self.directory)

    #Test that the function correctly writes the downloaded data to a file.
    def test_download_file(self):
        os.mkdir(self.directory)
        value = "covid"
        apiUrl='https://files.rcsb.org/download'
        tempgetpdbfiles(self.directoryWithOutMain, value, 'main/Search.json', apiUrl)
        filePath = os.path.join(self.directory, '6YUN.pdb')
        fileExists = os.path.exists(filePath)
        self.assertTrue(fileExists)
        for fileName in os.listdir(self.directory):
            filePath = os.path.join(self.directory, fileName)
            try:
                if os.path.isfile(filePath) or os.path.islink(filePath):
                    os.unlink(filePath)
                elif os.path.isdir(filePath):
                    shutil.rmtree(filePath)
            except Exception as e:
                print(f'Failed to delete {filePath}. Reason: {e}')
        os.rmdir(self.directory)

    #Test that the downloaded file contains the correct data and format.
    def test_file_format(self):
        os.mkdir(self.directory)
        value = "covid"
        apiUrl='https://files.rcsb.org/download'
        tempgetpdbfiles(self.directoryWithOutMain, value, 'main/Search.json', apiUrl)
        filePath = os.path.join(self.directory, '6YUN.pdb')
        with open(filePath, 'r') as f:
            content = f.read()
        self.assertTrue(content.startswith('HEADER'))
        for fileName in os.listdir(self.directory):
            filePath = os.path.join(self.directory, fileName)
            try:
                if os.path.isfile(filePath) or os.path.islink(filePath):
                    os.unlink(filePath)
                elif os.path.isdir(filePath):
                    shutil.rmtree(filePath)
            except Exception as e:
                print(f'Failed to delete {filePath}. Reason: {e}')
        os.rmdir(self.directory)

    #Test that the function returns the correct output for different input values.
    def test_input_values(self):
        os.mkdir(self.directory)
        values = ["covid", "v", "vinay"]
        apiUrl='https://files.rcsb.org/download'
        for value in values:
            result = tempgetpdbfiles(self.directoryWithOutMain, value, 'main/Search.json', apiUrl)
            self.assertIsNone(result)
            fileSize = os.path.getsize(self.directory)
            self.assertGreaterEqual(fileSize, 0)
            for fileName in os.listdir(self.directory):
                filePath = os.path.join(self.directory, fileName)
                try:
                    if os.path.isfile(filePath) or os.path.islink(filePath):
                        os.unlink(filePath)
                    elif os.path.isdir(filePath):
                        shutil.rmtree(filePath)
                except Exception as e:
                    print(f'Failed to delete {filePath}. Reason: {e}')
        os.rmdir(self.directory)

class Testemptypdbfolder(unittest.TestCase):
    def test_emptypdbfolder(self):
        with TemporaryDirectory() as temp_dir:
            file1 = os.path.join(temp_dir, 'file1.txt')
            file2 = os.path.join(temp_dir, 'file2.txt')
            sub_dir = os.path.join(temp_dir, 'subdir')
            os.mkdir(sub_dir)
            file3 = os.path.join(sub_dir, 'file3.txt')
            open(file1, 'w').close()
            open(file2, 'w').close()
            open(file3, 'w').close()

            main.emptypdbfolder(temp_dir)

            assert not os.path.exists(file1)
            assert not os.path.exists(file2)
            assert not os.path.exists(file3)