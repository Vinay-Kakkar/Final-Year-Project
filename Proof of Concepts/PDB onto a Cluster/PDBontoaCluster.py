from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os

def getallpdbfiles(directory):
    pdbfiles = []
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith(".pdb"):
            pdbfiles.append(filename)
    return pdbfiles

def listallpdbfiles(pdbfiles):
    pdbs = []
    for pdbfile in pdbfiles:
        filepath = (directory.decode('utf-8')+"/"+pdbfile)
        pdbs.append(readPDBfile(filepath))
    return pdbs

def convertPDBintoRDD(pdbs):
    rdds = []
    for pdb in pdbs:
        rdds.append(pdb.rdd.flatMap(lambda x: list(x)))
    return rdds

def convertrddsintodfs(rdds):
    dfs = []
    for rdd in rdds:
        dfs.append(spark.createDataFrame(rdd, StringType()))
    return dfs

def readPDBfile(fileloc):
    pdb = spark.read.option('header','true').csv(fileloc, inferSchema = True)
    return pdb

spark = SparkSession.builder.appName('PDB').getOrCreate()
directory = os.fsencode("Proof of Concepts/PDB onto a Cluster/Original PDBs")

pdbfiles = getallpdbfiles(directory)


pdbs = listallpdbfiles(pdbfiles)

rdds = convertPDBintoRDD(pdbs)

dfs = convertrddsintodfs(rdds)


for df in dfs:
    f = open("Proof of Concepts/PDB onto a Cluster/PDBs for Executables/newPDB.pdb", 'w')
    for i in df.collect():
        f.write(i[0]+"\n")
    os.system("wc -l Proof\ of\ Concepts/PDB\ onto\ a\ Cluster/PDBs\ for\ Executables/newPDB.pdb")
    f.close()



