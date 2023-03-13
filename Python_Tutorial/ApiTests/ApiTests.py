import requests
import json
import urllib.parse
from os.path import exists

with open(file='Python_Tutorial/ApiTests/Search.json', mode="r") as jsonFile:
    data = json.load(jsonFile)

search = input("Enter you search for pdb files: ")

data['query']['parameters']['value'] = search


with open(file='Python_Tutorial/ApiTests/Search.json', mode="w") as jsonFile:
    json.dump(data, jsonFile)

#fix to make the values in data not use ' qoutes but use "" qoutes instead
data = json.dumps(data)

newdata = urllib.parse.quote(data)
apicall = 'https://search.rcsb.org/rcsbsearch/v2/query?json={}'.format(newdata)


#Check this line to see what response you are getting if code stops working
result = requests.get(apicall)
result = result.json()

listofresults = []
for x in result["result_set"]:
    listofresults.append(x['identifier'])
#Eveything above is for getting the values of the search

for pdbfile in listofresults:
    if exists('Python_Tutorial/ApiTests/PDBfiles/' + pdbfile + '.pdb'):
        print(pdbfile + ': already exists')
        continue
    apicall = 'https://files.rcsb.org/download/{}.pdb'.format(pdbfile)

    response = requests.get(apicall)

    with open('Python_Tutorial/ApiTests/PDBfiles/' + pdbfile + '.pdb', 'wb') as f:
        f.write(response.content)

#503 Service Unavailable. The server is currently unable to handle the request due to a temporary overloading