## Second Problem: The Input.txt contains results of diagnosis of various medical test of different patients. The results are in binary where 1 indicates yes and 0 indicates No. The Reference file consists of test results of few people. 
## These people serve as a reference point.
## For each patient in Input file we wish to find the closest reference in Reference file.


from pyspark.sql import SparkSession

from pyspark import SparkContext

from difflib import SequenceMatcher

## Steps:

## Generate pairs using cartesian product.

## Compare each input file record with all reference file records.

## Display/Store output.


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
inputfile = sc.textFile("input.txt")
referencefile = sc.textFile("Reference.txt")




def func(values):
    return 1 if (SequenceMatcher(None, values[0].split(":")[1], values[1].split(":")[1]).ratio() > 0.8) else 0

print(inputfile.cartesian(referencefile).map(func).collect())