## First Problem: Count and print the number of three long consecutive words in a sentence that starts with the same english alphabet. We say that a word is long if it is greater than four alphabets.


from pyspark.sql import SparkSession

from pyspark import SparkContext


sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

## Starting off by reading the input file as:
inputfile = sc.textFile("./TXT.txt")

## First, we need to break the file into lines for which splitting on the base of “.” is required which is done as follows:
## inputfile.flatMap(lambda line: line.split("."))

## Next, to get words, we further need to split a line using “ ” which is done using:
## inputfile.flatMap(lambda line: line.split(".")).map(lambda line: line.split(" "))

## To get words having length greater than 4, we define a function as follows:
def func(lines):
    p = []
    for l in lines:
        if len(l)>4:
            p.append(l.lower())
    return p

## Next, we need to get only those words having length greater than 4 which can be done using the function defined above:
## inputfile.flatMap(lambda line: line.split(".")).map(lambda line: line.split(" ")).map(func)

## Next we need trigrams of consecutive words which can be generated using:
## inputfile.flatMap(lambda line: line.split(".")).map(lambda line: line.split(" ")).map(func).flatMap(lambda xs: (tuple(x) for x in zip(xs, xs[1:], xs[2:])))

## But we require only those trigrams having all words starting with the same alphabet for which we will use:
## inputfile.flatMap(lambda line: line.split(".")).map(lambda line: line.split(" ")).map(func).flatMap(lambda xs: (tuple(x) for x in zip(xs, xs[1:], xs[2:]) if (x[0][0] == x[1][0] == x[2][0])))

## Now we need to count the number of trigrams we got:
## inputfile.flatMap(lambda line: line.split(".")).map(lambda line: line.split(" ")).map(func).flatMap(lambda xs: (tuple(x) for x in zip(xs, xs[1:], xs[2:]) if (x[0][0] == x[1][0] == x[2][0]))).map(lambda x:(x[0][0], 1))
## The above code, adds a “1” corresponding each trigram catered while processing lines and sends it to the reducer.

## Now we need a reducer to collect the data sent from the mapper for which:
# #inputfile.flatMap(lambda line: line.split(".")).map(lambda line: line.split(" ")).map(func).flatMap(lambda xs: (tuple(x) for x in zip(xs, xs[1:], xs[2:]) if (x[0][0] == x[1][0] == x[2][0]))).map(lambda x:(x[0][0], 1)).reduceByKey(lambda accum, n: accum + n)


## Now, we need to display the output on screen using:
inputfile = inputfile.flatMap(lambda line: line.split(".")).map(lambda line: line.split(" ")).map(func).flatMap(lambda xs: (tuple(x) for x in zip(xs, xs[1:], xs[2:]) if (x[0][0] == x[1][0] == x[2][0]))).map(lambda x:(x[0][0], 1)).reduceByKey(lambda accum, n: accum + n).collect()

print ("This is the output" , inputfile)