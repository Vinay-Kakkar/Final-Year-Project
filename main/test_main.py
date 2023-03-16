import pdb
import unittest
import main
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
import pandas as pd
import os


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


    
