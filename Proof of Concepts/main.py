from pdb import Pdb
from pyspark.sql import SparkSession

import Bio
from Bio.PDB import PDBParser
import Bio.SeqRecord
import xpdb

spark = SparkSession.builder.appName('PDB').getOrCreate()

pdb = spark.read.option('header','true').csv('Proof of Concepts/AF-A0A452S449-F1-model_v3.pdb', inferSchema = True)

#pdb.printSchema()

#print("Type of datafram is: " , type(pdb))
#print("Columns in dataframe: " , pdb.columns)
#print("first 2 values in dataframe: " , pdb.head(2))

#Pdb.rdd.filter(lambda r: str(r['target']).startswith('good'))

#pdb_rdd = pdb.rdd.filter(lambda x: 'ATOM').take(5)

#print(pdb_rdd)

def extract_seqrecords(pdbcode, struct):
    """
    Extracts the sequence records from a Bio.PDB structure.
    :param pdbcode: the PDB ID of the structure, needed to add a sequence ID to the result
    :param struct: a Bio.PDB.Structure object
    :return: a list of Bio.SeqRecord objects
    """
    ppb = Bio.PDB.PPBuilder()
    seqrecords = []
    for i, chain in enumerate(struct.get_chains()):
        # extract and store sequences as list of SeqRecord objects
        pps = ppb.build_peptides(chain)    # polypeptides
        seq = pps[0].get_sequence() # just take the first, hope there's no chain break
        seqid = pdbcode + chain.id
        seqrec = Bio.SeqRecord.SeqRecord(seq, id=seqid, 
            description="Sequence #{}, {}".format(i+1, seqid))
        seqrecords.append(seqrec)
    return seqrecords

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

pdb_bio = read_pdb("A0A452S449", "Proof of Concepts/AF-A0A452S449-F1-model_v3.pdb")

pdb_seq = extract_seqrecords("A0A452S449", pdb_bio)

print(pdb_seq)
print(type(pdb_seq))