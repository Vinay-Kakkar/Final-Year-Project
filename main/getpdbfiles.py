import requests
import json
import urllib.parse
from os.path import exists
import sys

def main(folder, value):

    jsonfile = 'Search.json'
    directory = ("/Users/vinaykakkar/Desktop/PROJECT/main/"+folder+"/")


    with open(file=jsonfile, mode="r") as jsonFile:
        data = json.load(jsonFile)

    data['query']['parameters']['value'] = value


    with open(file=jsonfile, mode="w") as jsonFile:
        json.dump(data, jsonFile)

    #fix to make the values in data not use ' qoutes but use "" qoutes instead
    data = json.dumps(data)

    newData = urllib.parse.quote(data)
    apiCall = 'https://search.rcsb.org/rcsbsearch/v2/query?json={}'.format(newData)


    #Check this line to see what response you are getting if code stops working
    result = requests.get(apiCall)
    result = result.json()

    listOfResults = []
    for x in result["result_set"]:
        listOfResults.append(x['identifier'])
    #Eveything above is for getting the values of the search

    for pdbFile in listOfResults:
        if exists(directory + pdbFile + '.pdb'):
            print(pdbFile + ': already exists')
            continue
        apiCall = 'https://files.rcsb.org/download/{}.pdb'.format(pdbFile)

        response = requests.get(apiCall)

        with open(directory + pdbFile + '.pdb', 'wb') as f:
            f.write(response.content)

    #503 Service Unavailable. The server is currently unable to handle the request due to a temporary overloading


if __name__ == '__main__':
    globals()[sys.argv[1]](sys.argv[2], sys.argv[3])

#Example run line: python3 getpdbfiles.py main PDBsDirectory2 vinay