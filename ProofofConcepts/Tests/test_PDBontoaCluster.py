import unittest
import PDBontoaCluster.Lines
import os

print(__name__)

class TestGetAllPDBFiles(unittest.TestCase):
    def testgetPDBFile(self):
        directory = os.fsencode("Proof of Concepts/PDB onto a Cluster/Original PDBs")
        
        print(directory)
        test = PDBontoaCluster.Lines.getallpdbfiles(directory)

        test1 = [1,2]
        print(type(test))
        print(type(test1))
        self.assertEqual(type(test)), type(test1)
    