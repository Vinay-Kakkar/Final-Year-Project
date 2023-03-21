 # PDB Data Analysis Tool #

 This project is a Python-based tool for analyzing and processing PDB (Protein Data Bank) files. It includes several functions for working with PDB files, such as counting the number of lines in a file, running TMalign on pairs of files, and downloading PDB files from the RCSB (Research Collaboratory for Structural Bioinformatics) database.

## Requirements ##

* PySpark
* TM-align (https://zhanglab.ccmb.med.umich.edu/TM-align/)
* requests
* json
* urllib.parse
* os
* pathlib
* tempfile
* sys
* re

## Installation ##

1. Clone the repository using the following command:
```
git clone https://github.com/<username>/<project-name>.git
```

2. Install the required packages:
```
pip install pyspark requests json urllib pathlib
```

3. Download the TMalign executable file and add it to the project directory:
```
wget http://zhanglab.ccmb.med.umich.edu/TM-align/TMalign.gz
gunzip TMalign.gz
chmod +x TMalign
```


## Usage ##

To use the tool, run the main.py file with the desired function and arguments. For example:

### Line Count ###

To count the number of lines in a file, use the following command:

```
python3 main.py linecount <directory_path>
```

For example:

```
python3 main.py linecount PDBsDirectory1
```

### TM-align ###

To perform TM-align between two PDB files, use the following command:

```
python3 main.py tmalign <directory_path_1> <directory_path_2>
```

For example:

```
python3 main.py tmalign PDBsDirectory1 PDBsDirectory2
```

### Search for PDB files ###

To search for PDB files with a given query string, use the following command:

```
python3 main.py searchpdbfiles <query_string>
```

For example:

```
python3 main.py searchpdbfiles vinay
```

### Get PDB files ###

To download PDB files with a given query string and store them in a directory, use the following command:

```
python3 main.py getpdbfiles <directory_path> <query_string>
```

For example:

```
python3 main.py getpdbfiles PDBsDirectory1 vinay 
```

### Empty PDB Folders ###

To Empty all the pdbs in a provided directery, use the following command:

```
python3 main.py emptypdbfolder <directory_path>
```

For example:

```
python3 main.py emptypdbfolder PDBsDirectory1
```

### Get Current PDB Files ###

To get all the present pdb files in a provided directery, use the following command:

```
python3 main.py getcurrentpdbfiles <directory_path>
```

For example:

```
python3 main.py getcurrentpdbfiles PDBsDirectory1
```


Thank you for taking the time to read this README. I hope it has provided you with a clear understanding of the project and how to get started. If you have any questions or feedback, please don't hesitate to reach out. Contributions are always welcome, so if you would like to get involved in the development of this project, feel free to submit a pull request. Again, thank you for your interest and I look forward to hearing from you!