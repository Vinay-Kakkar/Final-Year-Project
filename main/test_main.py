import pdb
import unittest
import main
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
import pandas as pd
import os

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
        
        