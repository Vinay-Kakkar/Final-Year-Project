from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
from pathlib import Path

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
    pdb = spark.read.option('header','false').csv(fileloc, inferSchema = True)
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

def numberoflinesexecutables():
    # This is an example execuatble that returns the number of lines within each pdb provided
    os.system("wc -l /Users/vinaykakkar/Desktop/PROJECT/ProofofConcepts/PDBontoaCluster/PDBsforExecutables/newPDB.pdb")

def main(spark):

    directory = ("/Users/vinaykakkar/Desktop/PROJECT/ProofofConcepts/PDBontoaCluster/OriginalPDBs")

    pdbfiles = getallpdbfiles(directory)

    pdbs = listallpdbfiles(pdbfiles, directory, spark)

    rdds = convertPDBintoRDD(pdbs, spark)

    # In order to run an executable we first need to translate the type into a dataframe
    dfs = convertrddsintodfs(rdds, spark)

    executable = numberoflinesexecutables

    # before running the executable we need to convert the dataframe back into a pdb file
    convertdftopdb(dfs, spark, executable)



    ## Test this way of converting datframe to file https://stackoverflow.com/questions/67316136/spark-write-dataframe-with-custom-file-name
if __name__ == '__main__':
    # This creates a local cluster
    spark = SparkSession.builder.appName('PDB').getOrCreate()
    main(spark)


