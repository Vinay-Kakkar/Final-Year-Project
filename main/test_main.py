import pdb
import shutil
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
def templinecount(spark, filename):
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+filename+"/*")

    if not os.path.exists("main/"+filename):
        raise Exception("Path does not exist")

    files = os.listdir("main/"+filename)
    if not files:
        raise Exception("No files found in folder provided")
    rddkeyvalue = spark.sparkContext.wholeTextFiles(directory)
    def numberoflinesinfile(k):
        os.system("wc -l "+ k[45:])
    rddkeyvalue.map(lambda x: numberoflinesinfile(x[0])).collect()
    return True

def tempsearchpdbfiles(value, jsonpath, api):
    # python3 main.py searchpdbfiles vinay
    if bool(re.search('[!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]', value)):
        raise Exception("Invalid Input please dont use punctuations")
    jsonfile = jsonpath
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
    newdata = urllib.parse.quote(data)
    apicall = (api+'json={}'.format(newdata))
    #Check this line to see what response you are getting if code stops working
    result = requests.get(apicall)
    if result.status_code != 200:
        raise Exception("API error occurred")
    resultjson = result.json()
    listofresults = []
    for result in resultjson["result_set"]:
        listofresults.append(result)
    print(listofresults)
    return listofresults

def tempgetpdbfiles(folder, value, jsonpath, api):
    if bool(re.search('[!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]', value)):
        raise Exception("Invalid Input please dont use punctuations")
    jsonfile = jsonpath
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
    newdata = urllib.parse.quote(data)
    apicall = 'https://search.rcsb.org/rcsbsearch/v2/query?json={}'.format(newdata)
    result = requests.get(apicall)
    if result.status_code != 200:
        raise Exception("API error occurred")
    result = result.json()
    listofresults = []
    for x in result["result_set"]:
        listofresults.append(x['identifier'])
    #Eveything above is for getting the values of the search

    for pdbfile in listofresults:
        if exists(directory + pdbfile + '.pdb'):
            print(pdbfile + ': already exists')
            continue
        apicall = api+'/{}.pdb'.format(pdbfile)
        try:
            response = requests.get(apicall)
        except:
            raise Exception("API error occurred")
        with open(directory + pdbfile + '.pdb', 'wb') as f:
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
        subdirectory = "subfolder"
        os.mkdir(self.directory)
        open(os.path.join(self.directory, "fake1.pdb"), "w").close()
        os.mkdir(os.path.join(self.directory, subdirectory))
        open(os.path.join(self.directory, subdirectory, "fake2.pdb"), "w").close()


        test = main.getcurrentpdbfiles(self.directory)
        self.assertTrue(len(test) is 2)

        os.remove(os.path.join(self.directory, "fake1.pdb"))
        os.remove(os.path.join(self.directory, subdirectory, "fake2.pdb"))
        os.rmdir(os.path.join(self.directory, subdirectory))
        os.rmdir(self.directory)
    
    #Test that the function only returns files and not directories or other types of files (e.g., symbolic links).
    def test_subdir(self):
        subdirectory = "subfolder"
        os.mkdir(self.directory)
        open(os.path.join(self.directory, "fake1.pdb"), "w").close()
        os.mkdir(os.path.join(self.directory, subdirectory))
        open(os.path.join(self.directory, subdirectory, "fake2.pdb"), "w").close()

        os.symlink(os.path.join(self.directory, "file1.txt"), os.path.join(self.directory, "link1"))
        os.symlink(os.path.join(self.directory, "subdirectory"), os.path.join(self.directory, "link2"))


        test = main.getcurrentpdbfiles(self.directory)
        self.assertTrue(len(test) is 2)

        os.remove(os.path.join(self.directory, "fake1.pdb"))
        os.remove(os.path.join(self.directory, "link1"))
        os.remove(os.path.join(self.directory, "link2"))
        os.remove(os.path.join(self.directory, subdirectory, "fake2.pdb"))
        os.rmdir(os.path.join(self.directory, subdirectory))
        os.rmdir(self.directory)
   
class Testlinecount(unittest.TestCase):
    directory = ("main/testdirectory")
    directorywithoutmain = ("testdirectory")

    #Test that the function returns Exception when there is no files in folder
    def test_empty_folder_path(self):
        with self.assertRaises(Exception) as context:
            spark = SparkSession.builder.appName('Test').getOrCreate()
            os.mkdir(self.directory)
            test = templinecount(spark, self.directorywithoutmain)
        self.assertTrue("No files found in folder provided" in str(context.exception))
        
        os.rmdir(self.directory)

    #Test that the function returns true when given a folder with a file:
    def test_folder_with_file(self):
        spark = SparkSession.builder.appName('Test').getOrCreate()
        os.mkdir(self.directory)
        open(os.path.join(self.directory, "fake.pdb"), "w").close()
        test = templinecount(spark, self.directorywithoutmain)
        self.assertTrue(test is True)
        
        os.remove(os.path.join(self.directory, "fake.pdb"))
        os.rmdir(self.directory)

    #Test that the function returns true when given a folder with multiple files:
    def test_folder_with_multiple_file(self):

        spark = SparkSession.builder.appName('Test').getOrCreate()

        subdirectory = "subfolder"
        os.mkdir(self.directory)
        open(os.path.join(self.directory, "fake1.pdb"), "w").close()
        os.mkdir(os.path.join(self.directory, subdirectory))
        open(os.path.join(self.directory, subdirectory, "fake2.pdb"), "w").close()

        test = templinecount(spark, self.directorywithoutmain)
        self.assertTrue(test is True)
        
        os.remove(os.path.join(self.directory, "fake1.pdb"))
        os.remove(os.path.join(self.directory, subdirectory, "fake2.pdb"))
        os.rmdir(os.path.join(self.directory, subdirectory))
        os.rmdir(self.directory)
    
    #Test that the function returns exception when there is a invalid folder
    def test_invalid_folder(self):
        spark = SparkSession.builder.appName('Test').getOrCreate()
        with self.assertRaises(Exception) as context:
            test = templinecount(spark, self.directorywithoutmain)
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
    directorywithoutmain = ("testdirectory")
    #Test that the function raises an error when an invalid input is passed.
    def test_invalidinput(self):
        os.mkdir(self.directory)
        value = "___"
        with self.assertRaises(Exception) as context:
            test = tempgetpdbfiles(self.directorywithoutmain, value, 'main/Search.json', 'https://search.rcsb.org/rcsbsearch/v2/query?')
        self.assertTrue("Invalid Input please dont use punctuations" in str(context.exception))

        os.rmdir(self.directory)
    
    #Test that the function successfully reads and loads the JSON file.
    def test_valid_json(self):
        os.mkdir(self.directory)
        value = "covid"
        result = tempgetpdbfiles(self.directorywithoutmain, value, 'main/Search.json', 'https://search.rcsb.org/rcsbsearch/v2/query?')
        self.assertIsNone(result)
        for filename in os.listdir(self.directory):
            file_path = os.path.join(self.directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
        os.rmdir(self.directory)
    
    
    #Test that the function successfully receives a response from the API.
    def test_api_response(self):
        os.mkdir(self.directory)
        value = "covid"
        api_url='https://files.rcsb.org/download'
        response = tempgetpdbfiles(self.directorywithoutmain, value, 'main/Search.json', api_url)
        self.assertIsNone(response)
        for filename in os.listdir(self.directory):
            file_path = os.path.join(self.directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
        os.rmdir(self.directory)
    
    #Test that the function correctly handles any errors or exceptions that may occur during the API call (e.g. network errors, server errors).
    def test_api_call_error_handling(self):
        os.mkdir(self.directory)
        value = "covid"
        api_url='https://files.rcsb.'
        with self.assertRaises(Exception) as context:
            tempgetpdbfiles(self.directorywithoutmain, value, 'main/Search.json', api_url)
        self.assertTrue("API error occurred" in str(context.exception))
        for filename in os.listdir(self.directory):
            file_path = os.path.join(self.directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
        os.rmdir(self.directory)

    #Test that the function correctly parses and returns the data from the API response.
    def test_pdb_content(self):
        os.mkdir(self.directory)
        value = "covid"
        api_url='https://files.rcsb.org/download'
        result = tempgetpdbfiles(self.directorywithoutmain, value, 'main/Search.json', api_url)
        self.assertIsNone(result)
        file_path = os.path.join(self.directory, '6YUN.pdb')
        with open(file_path, 'r') as f:
            content = f.read()
        self.assertTrue(content.startswith('HEADER'))
        for filename in os.listdir(self.directory):
            file_path = os.path.join(self.directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
        os.rmdir(self.directory)

    #Test that the function correctly writes the downloaded data to a file.
    def test_download_file(self):
        os.mkdir(self.directory)
        value = "covid"
        api_url='https://files.rcsb.org/download'
        tempgetpdbfiles(self.directorywithoutmain, value, 'main/Search.json', api_url)
        file_path = os.path.join(self.directory, '6YUN.pdb')
        file_exists = os.path.exists(file_path)
        self.assertTrue(file_exists)
        for filename in os.listdir(self.directory):
            file_path = os.path.join(self.directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
        os.rmdir(self.directory)

    #Test that the downloaded file contains the correct data and format.
    def test_file_format(self):
        os.mkdir(self.directory)
        value = "covid"
        api_url='https://files.rcsb.org/download'
        tempgetpdbfiles(self.directorywithoutmain, value, 'main/Search.json', api_url)
        file_path = os.path.join(self.directory, '6YUN.pdb')
        with open(file_path, 'r') as f:
            content = f.read()
        self.assertTrue(content.startswith('HEADER'))
        for filename in os.listdir(self.directory):
            file_path = os.path.join(self.directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f'Failed to delete {file_path}. Reason: {e}')
        os.rmdir(self.directory)

    #Test that the function returns the correct output for different input values.
    def test_input_values(self):
        os.mkdir(self.directory)
        values = ["covid", "v", "vinay"]
        api_url='https://files.rcsb.org/download'
        for value in values:
            result = tempgetpdbfiles(self.directorywithoutmain, value, 'main/Search.json', api_url)
            self.assertIsNone(result)
            file_size = os.path.getsize(self.directory)
            self.assertGreaterEqual(file_size, 0)
            for filename in os.listdir(self.directory):
                file_path = os.path.join(self.directory, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(f'Failed to delete {file_path}. Reason: {e}')
        os.rmdir(self.directory)
