from pdb import Pdb
from pyspark.sql import SparkSession

import Bio
from Bio.PDB import PDBParser
import Bio.SeqRecord
import xpdb
import os

def read_pdb(pdbcode, pdbfilenm):
    """
    Read a PDB structure from a file.
    :param pdbcode: A PDB ID string
    :param pdbfilenm: The PDB file
    :return: a Bio.PDB.Structure object or None if something went wrong
    """
    try:
        pdbparser = Bio.PDB.PDBParser(QUIET=True)   # suppress PDBConstructionWarning
        struct = pdbparser.get_structure(pdbcode, pdbfilenm)
        return struct
    except Exception as err:
        print(str(err))
        return None

pdb_bio = read_pdb("A0A452S449", "Proof of Concepts/PDB into rdd/pdb.pdb")

structurefile = xpdb.SloppyPDBIO()
structurefile.set_structure(pdb_bio)
if os.path.exists("Proof of Concepts/PDB into rdd/.pdb"):
    print('It exists')
else:
    structurefile.save("Proof of Concepts/PDB into rdd/new_pdb.pdb")


spark = SparkSession.builder.appName('PDB').getOrCreate()

pdb = spark.read.option('header','true').csv("Proof of Concepts/PDB into rdd/new_pdb.pdb", inferSchema = True)

inputfile = pdb.rdd.flatMap(lambda line: print(line[:4])).take(5)

print(inputfile)
