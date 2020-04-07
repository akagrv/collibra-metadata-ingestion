# -*- coding: utf-8 -*-
"""
Modified on Mon Apr 6 13:35:10 2020

@author: gagrawa3
"""

import json
import json_object_parser as parser

# create a json obj to store the parsed results
json_obj = []

# create community, domain and asset object
community_list = []
domain_list = []
asset_list = []

def ingest_column(communityName, domainName, results, session, parentCommunityName = None):
    
    community_list.append(communityName)
    domain_list.append((communityName, domainName, 'Physical Data Dictionary'))

    for schemaName, tableName, columnName, dataType in results:
        # (relation_type_id:relation_direction, relation_asset_name)
        relations = ('00000000-0000-0000-0000-000000007042:TARGET', schemaName+'__'+tableName)
        attributes_list = (('Column Data Type', dataType))
        asset_list.append((communityName, domainName, schemaName+'__'+tableName+'__'+columnName, 'Column', columnName, attributes_list, relations))
   
    for community in community_list:
        json_obj.append(parser.getCommunityObj(community, parentCommunityName))
    
    for communityName, domainName, domainType in domain_list:
        json_obj.append(parser.getDomainObj(communityName, domainName, domainType))
    
    for communityName, domainName, assetName, assetType, columnName, attributes_list, relation in asset_list:
        json_obj.append(parser.getAssetObj(communityName, domainName, assetName, assetType, columnName, attributes_list, relation))

    
    with open("column_template.json", "w") as write_file:
        json.dump(json_obj, write_file)
    
    url = 'https://asu-dev.collibra.com/rest/2.0/import/json-job'
    
    files = {'file': open('column_template.json', 'rb')}
    payload = {'sendNotification':'true'}

    response = session.post(url, files=files, data=payload)
    
    # print(response.request.headers)
    
    if response:
        print(response.json())
    else:
        print(response.text)