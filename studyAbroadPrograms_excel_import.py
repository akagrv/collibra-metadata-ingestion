import xlrd
import json
import json_object_parser as parser
import requests
from getpass import getpass

path = ('StudyAbroadPrograms.xlsx')

# open workbook
wb = xlrd.open_workbook(path)
sheet = wb.sheet_by_index(0)

# create a json obj out of the parsed rows
json_obj = []

community_list = set()
domain_list = set()
asset_list = set()

headers = sheet.row_values(0)

for i in range(1,sheet.nrows):
    row = sheet.row_values(i)
    community_list.add('StudyAbroadProgramsCommunity')
    domain_list.add(('StudyAbroadProgramsCommunity', 'Physical Data Dictionary Domain', 'Physical Data Dictionary'))
    attributes_list = ((headers[1],row[1]),(headers[2],row[2]),(headers[3],row[3]))
    asset_list.add(('StudyAbroadProgramsCommunity', 'Physical Data Dictionary Domain', row[0], 'Column', attributes_list))

for community in community_list:
    json_obj.append(parser.getCommunityObj(community))

for communityName, domainName, domainType in domain_list:
    json_obj.append(parser.getDomainObj(communityName, domainName, domainType))

for communityName, domainName, assetName, assetType, attributes_list in asset_list:
    json_obj.append(parser.getAssetObj(communityName, domainName, assetName, assetType, attributes_list))

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError

# print(json.dumps(json_obj,default=set_default))

with open("study_abroad_programs_json_file.json", "w") as write_file:
    json.dump(json_obj, write_file, default=set_default)

url = 'https://asu-dev.collibra.com/rest/2.0/import/json-job'

files = {'file': open('study_abroad_programs_json_file.json', 'rb')}

session = requests.Session()
session.auth = ('gagrawa3', getpass())
response = session.post(url, files=files)

# print(response.request.headers)

if response:
    print(response.json())
else:
    print(response.text)