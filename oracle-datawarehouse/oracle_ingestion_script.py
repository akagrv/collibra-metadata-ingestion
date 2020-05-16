# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 13:27:48 2020

@author: gagrawa3
"""

import requests
import oracle_connect
import create_schema
import create_table
import create_column
import configparser
import os

config = configparser.ConfigParser(os.environ)
config.read('db.ini')

username = config['collibra']['username']
password = config['collibra']['password']

# create a session to be used by all the requests
with requests.Session() as session:
    session.auth = (username, password)
    
# get cursor object for querying Oracle
cur = oracle_connect.getCursor()

parentCommunityName = 'Data Warehouse'
communityName = 'Oracle Data Warehouse'
domainName = 'Oracle Physical Data Dictionary'

# query to get metadata from table
query = ''' select col.owner as schema_name,
               col.table_name,
               col.column_name,
               col.DATA_TYPE
            from dba_tab_columns col
            inner join dba_tables t on col.owner = t.owner
                                          and col.table_name = t.table_name
                                          and t.owner in (
                                          'SYSADM',
                                          'ASUDW',
                                          'DIRECTORY',
                                          'ASU_CENSUS'
                                          )
            order by col.owner, col.table_name, col.column_id '''


cur.execute(query)
results = cur.fetchall()

# results field order
# schemaName, tableName, colName, colType 
schemaList = []
tableList = []
colList = []
colAttrList = []
for result in results:
    schemaList.append(result[0])
    tableList.append(result[1])
    colList.append(result[2])
    colAttrList.append(('Column Data Type', result[3]))
    
create_schema.ingest_schema(communityName, domainName, schemaList, session, parentCommunityName)
create_table.ingest_table(communityName, domainName, schemaList, tableList, None, session, parentCommunityName)
create_column.ingest_column(communityName, domainName, schemaList, tableList, colList, colAttrList, session, parentCommunityName)