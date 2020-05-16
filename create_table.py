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
asset_set = set()
asset_list = []

def ingest_table(communityName, domainName, schemaList, tableList, attrList, session, parentCommunityName = None):
    
    community_list.append(communityName)
    domain_list.append((communityName, domainName, 'Physical Data Dictionary'))
    
    if schemaList != None:
        print("Schema List is not null")
        # adds all the table under the given schema to the asset list
        if attrList != None:
            print("Table attributes is not null")
            for schemaName, tableName, tableAttr in zip(schemaList, tableList, attrList):
                # (relation_type_id:relation_direction, relation_asset_name) - (schema type id , schema name)
                relations = ('00000000-0000-0000-0000-000000007043:SOURCE',schemaName)
                asset_set.add((communityName, domainName, schemaName+'__'+tableName, 'Table', tableName, tableAttr, relations))
        else:
            print("Table Attributes list is null")
            for schemaName, tableName in zip(schemaList, tableList):
                # (relation_type_id:relation_direction, relation_asset_name) - (schema type id , schema name)
                relations = ('00000000-0000-0000-0000-000000007043:SOURCE',schemaName)
                asset_set.add((communityName, domainName, schemaName+'__'+tableName, 'Table', tableName, None, relations))
    else:
        print("Schema List is null")
        # adds all the table to the asset list
        if attrList != None:
            print("Table attributes is not null")
            for tableName, tableAttr in zip(tableList, attrList):
                asset_set.add((communityName, domainName, tableName, 'Table', tableName, tableAttr, None))
        else:
            print("Table attributes is null")
            for tableName in tableList:
                asset_set.add((communityName, domainName, tableName, 'Table', tableName, None, None))
    
    asset_list = list(asset_set)
    
    for community in community_list:
        json_obj.append(parser.getCommunityObj(community, parentCommunityName))
    
    for communityName, domainName, domainType in domain_list:
        json_obj.append(parser.getDomainObj(communityName, domainName, domainType))
    
    for communityName, domainName, assetName, assetType, tableName, attrList, relation in asset_list:
        json_obj.append(parser.getAssetObj(communityName, domainName, assetName, assetType, tableName, attrList, relation))
        
    with open("table_template.json", "w") as write_file:
        json.dump(json_obj, write_file)
    
    url = 'https://asu-dev.collibra.com/rest/2.0/import/json-job'
    # url = 'https://asu.collibra.com/rest/2.0/import/json-job'
    
    files = {'file': open('table_template.json', 'rb')}
    payload = {'sendNotification':'true'}
    
    response = session.post(url, files=files, data=payload)
    
    if response:
        print(response.json())
    else:
        print(response.text)