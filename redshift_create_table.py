import json
import json_object_parser as parser

# create a json obj to store the parsed results
json_obj = []

# create community, domain and asset object
community_list = set()
domain_list = set()
asset_list = set()

def ingest_table(communityName, domainName, schemaList, tableList, session):
    
    community_list.add(communityName)
    domain_list.add((communityName, domainName, 'Physical Data Dictionary'))
    
    # adds all the table under the given schema to the asset list
    for schemaName, tableName in zip(schemaList, tableList):
        # (relation_type_id:relation_direction, relation_asset_name) - (schema type id , schema name)
        relations = ('00000000-0000-0000-0000-000000007043:SOURCE',schemaName)
        asset_list.add((communityName, domainName, schemaName+'__'+tableName, 'Table', tableName, relations))

    for community in community_list:
        json_obj.append(parser.getCommunityObj(community, 'Data Warehouse'))
    
    for communityName, domainName, domainType in domain_list:
        json_obj.append(parser.getDomainObj(communityName, domainName, domainType))
    
    for communityName, domainName, assetName, assetType, tableName, relation in asset_list:
        json_obj.append(parser.getAssetObj(communityName, domainName, assetName, assetType, tableName, None, relation))
    
    def set_default(obj):
        if isinstance(obj, set):
            return list(obj)
        raise TypeError
        
    with open("redshift_table_template.json", "w") as write_file:
        json.dump(json_obj, write_file, default=set_default)
    
    url = 'https://asu-dev.collibra.com/rest/2.0/import/json-job'
    
    files = {'file': open('redshift_table_template.json', 'rb')}
    
    response = session.post(url, files=files)
    
    # print(response.request.headers)
    
    if response:
        print(response.json())
    else:
        print(response.text)