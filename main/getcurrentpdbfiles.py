import os
import sys

def main(folderPath):
    for file in os.listdir(folderPath):
        print(file)

if __name__ == '__main__':
    globals()[sys.argv[1]](sys.argv[2])
#Example run line: python3 getcurrentpdbfiles.py main PDBsDirectory1