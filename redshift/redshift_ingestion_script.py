# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 12:30:59 2020

@author: gagrawa3
"""

import requests
import redshift_connect
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
    
# get cursor object for querying Redshift
cur = redshift_connect.getCursor()

parentCommunityName = 'Data Warehouse'
communityName = 'Redshift Data Warehouse'
domainName = 'Redshift Physical Data Dictionary'

# query to get metadata from table
query = '''select
       sch.nspname as schemaname,
       tbl.relname as tablename, 
       col.attname as columnname,
       typname as data_type
    FROM pg_class tbl
    JOIN pg_attribute col ON tbl.oid = col.attrelid
    JOIN pg_namespace sch ON tbl.relnamespace = sch.oid
    JOIN pg_type tp on col.atttypid = tp.oid
    LEFT JOIN pg_constraint pk on tbl.oid = pk.conrelid and col.attnum = ANY(pk.conkey)
    WHERE col.attisdropped = false  --exclude dropped columns
             and col.attnum > 0 --exclude system columns (not part of user ddl)
             and tbl.relkind = 'r' --include only user tables
             and sch.nspname like 'asu_%' '''

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