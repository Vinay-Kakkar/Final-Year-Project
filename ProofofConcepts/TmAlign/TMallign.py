from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
from pathlib import Path
import time

def getallpdbfiles(directory):
    pdbfiles = []
    if (Path(directory).is_dir()):
        for file in os.listdir(directory):
            filename = os.fsdecode(file)
            if filename.endswith(".pdb"):
                pdbfiles.append(filename)
    else:
        raise Exception("No files found: check Directory")
    return pdbfiles

def listallpdbfiles(pdbfiles, directory, spark):
    pdbs = []
    for pdbfile in pdbfiles:
        filepath = (directory+ "/" +pdbfile)
        pdbs.append(readPDBfile(filepath, spark))
    return pdbs

def convertPDBintoRDD(pdbs, spark):
    rdds = []
    for pdb in pdbs:
        rdds.append(pdb.rdd.flatMap(lambda x: list(x)))
    return rdds

def convertrddsintodfs(rdds, spark):
    dfs = []
    for rdd in rdds:
        dfs.append(spark.createDataFrame(rdd, StringType()))
    return dfs

def readPDBfile(fileloc, spark):
    # In order to distriubte the files into a rdd we first need to create dataframes
    # We can read the pdb file as a csv as we only care about stripping the file line by line rather then sections within the lines
    #pdb = spark.read.option('header','false').csv(fileloc, inferSchema = True)
    pdb = spark.read.option('header','false').text(fileloc)
    return pdb

def convertdftopdb(dfs, spark, executable):
    for df in dfs:
        f = open("/Users/vinaykakkar/Desktop/PROJECT/ProofofConcepts/PDBontoaCluster/PDBsforExecutables/newPDB.pdb", 'w')
        for i in df.collect():
            if ("END " in i[0]):
                f.write(i[0])
            else:
                f.write(i[0]+"\n")
        f.close()
        executable()

start = time.time()
spark = SparkSession.builder.appName('PDB').getOrCreate()

directory1 = ("/Users/vinaykakkar/Desktop/PROJECT/ProofofConcepts/TmAlign/FirstPDB")

directory2 = ("/Users/vinaykakkar/Desktop/PROJECT/ProofofConcepts/TmAlign/SecondPDB")

Firstpdbfile = getallpdbfiles(directory1)
Secondpdbfile = getallpdbfiles(directory2)

pdbs1 = listallpdbfiles(Firstpdbfile, directory1, spark)
pdbs2 = listallpdbfiles(Secondpdbfile, directory2, spark)

rdd1 = convertPDBintoRDD(pdbs1, spark)
rdd2 = convertPDBintoRDD(pdbs2, spark)

dfs1 = convertrddsintodfs(rdd1, spark)
dfs2 = convertrddsintodfs(rdd2, spark)


for df in dfs1:
    f = open("/Users/vinaykakkar/Desktop/PROJECT/ProofofConcepts/TmAlign/TempPDB1/First.pdb", 'w')
    for i in df.collect():
        if ("END " in i[0]):
              f.write(i[0])
        else:
            f.write(i[0]+"\n")
    f.close()

    for df in dfs2:
        f = open("/Users/vinaykakkar/Desktop/PROJECT/ProofofConcepts/TmAlign/TempPDB2/Second.pdb", 'w')
        for i in df.collect():
            if ("END " in i[0]):
                f.write(i[0])
            else:
                f.write(i[0]+"\n")
        f.close()
        print("Executing..............")
        os.system("./TMalign TempPDB1/First.pdb TempPDB2/Second.pdb")

    
    end = time.time()
    print(end - start)