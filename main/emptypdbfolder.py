import os
import sys

def main(folder_path):
    # iterate through all files and subdirectories in the provided folder
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            # delete the file
            os.remove(os.path.join(root, file))

if __name__ == '__main__':
    globals()[sys.argv[1]](sys.argv[2])

#Example run line: python3 emptypdbfolder.py main PDBsDirectory1   