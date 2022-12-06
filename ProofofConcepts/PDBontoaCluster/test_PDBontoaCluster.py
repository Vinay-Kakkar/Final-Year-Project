import unittest
import Lines
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.rdd import RDD
import pandas as pd

class TestGetAllPDBFiles(unittest.TestCase):
    directory = ("/Users/vinaykakkar/Desktop/PROJECT-main/ProofofConcepts/PDBontoaCluster/OriginalPDBs")


    def test_checktypeofpdbs(self):
        test = Lines.getallpdbfiles(self.directory)
        lis = []
        self.assertTrue(type(test) is type(lis))
    
    def test_checktypeofpdb(self):
        test = Lines.getallpdbfiles(self.directory)
        string = "string"
        self.assertTrue(type(test[0]) is type(string))

    def test_checknumberofpdbs(self):
        test = Lines.getallpdbfiles(self.directory)
        self.assertEqual(len(test), 2)

    def test_checkwrongdirectory(self):
        directory = ("Wrong")
        with self.assertRaises(Exception) as context:
            Lines.getallpdbfiles(directory)
        self.assertTrue("No files found: check Directory" in str(context.exception))


class TestListAllPDBFiles(unittest.TestCase):
    directory = ("/Users/vinaykakkar/Desktop/PROJECT-main/ProofofConcepts/PDBontoaCluster/OriginalPDBs")
    
    def test_checklength(self):
        spark = SparkSession.builder.appName('Test').getOrCreate()
        pdbfiles = Lines.getallpdbfiles(self.directory)
        test = Lines.listallpdbfiles(pdbfiles, self.directory, spark)
        self.assertEqual(len(test), 2)

    def test_checktypeofpdb(self):
        spark = SparkSession.builder.appName('Test').getOrCreate()
        pdbfiles = Lines.getallpdbfiles(self.directory)
        test = Lines.listallpdbfiles(pdbfiles, self.directory, spark)

        data = [[1, "vinay"], [2, "vinay 2"], [3, "vinay again!"]]
        pdf = pd.DataFrame(data, columns=["id", "name"])

        df1 = spark.createDataFrame(pdf)
        self.assertEqual(isinstance(test[0], DataFrame), isinstance(df1, DataFrame))
        self.assertNotEqual(isinstance(test[0], RDD), isinstance(df1, DataFrame))

    def test_checkfirstvaluewithsecondvalue(self):
        spark = SparkSession.builder.appName('Test').getOrCreate()
        pdbfiles = Lines.getallpdbfiles(self.directory)
        test = Lines.listallpdbfiles(pdbfiles, self.directory, spark)

        self.assertNotEqual(test[0], test[1])
