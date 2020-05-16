# -*- coding: utf-8 -*-
"""
Modified on Mon Apr 6 13:35:10 2020

@author: gagrawa3
"""

import json
import json_object_parser as parser
import sys

# create a json obj to store the parsed results
list_json_obj = []
json_obj = []

# create community, domain and asset object
community_list = []
domain_list = []
asset_set = set()
asset_list = []

def ingest_column(communityName, domainName, schemaList, tableList, colList, attrList, session, parentCommunityName = None):
    global json_obj
    community_list.append(communityName)
    domain_list.append((communityName, domainName, 'Physical Data Dictionary'))

    if schemaList != None:
        for schemaName, tableName, columnName, columnAttr in zip(schemaList, tableList, colList, attrList):
            # (relation_type_id:relation_direction, relation_asset_name)
            relations = ('00000000-0000-0000-0000-000000007042:TARGET', schemaName+'__'+tableName)
            asset_set.add((communityName, domainName, schemaName+'__'+tableName+'__'+columnName, 'Column', columnName, columnAttr, relations))
    else:
        for tableName, columnName, columnAttr in zip(tableList, colList, attrList):
            # (relation_type_id:relation_direction, relation_asset_name)
            relations = ('00000000-0000-0000-0000-000000007042:TARGET', tableName)
            asset_set.add((communityName, domainName, tableName+'__'+columnName, 'Column', columnName, columnAttr, relations))
       
    asset_list = list(asset_set)
    
    for community in community_list:
        json_obj.append(parser.getCommunityObj(community, parentCommunityName))
    
    for communityName, domainName, domainType in domain_list:
        json_obj.append(parser.getDomainObj(communityName, domainName, domainType))
    
    for communityName, domainName, assetName, assetType, columnName, attrList, relation in asset_list:
        # split if file size is roughly around 20MB
        if(sys.getsizeof(json_obj) > 350000) :
            list_json_obj.append(json_obj)
            json_obj = []
        json_obj.append(parser.getAssetObj(communityName, domainName, assetName, assetType, columnName, attrList, relation))

    list_json_obj.append(json_obj)
    for index, json_obj in enumerate(list_json_obj,1):
        with open(f"column_template_{index}.json", "w") as write_file:
            json.dump(json_obj, write_file)
        
        
        print(f"Ingesting data from template file: column_template_{index}.json")
        url = 'https://asu-dev.collibra.com/rest/2.0/import/json-job'
        # url = 'https://asu.collibra.com/rest/2.0/import/json-job'
        
        files = {'file': open(f'column_template_{index}.json', 'rb')}
        payload = {'sendNotification':'true'}
    
        response = session.post(url, files=files, data=payload)
        
        # print(response.request.headers)
        
        if response:
            print(response.json())
        else:
            print(response.text)