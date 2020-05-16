# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 16:02:16 2020

@author: gagrawa3
"""

import requests
import microsoft_sql_server_connect
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
    
# get cursor object for querying Microsoft SQL Server
cur = microsoft_sql_server_connect.getCursor()

parentCommunityName = 'OKED'
communityName = 'Tableau MySQL Database'
domainName = 'Tableau MySQL PDD'

# query to get metadata from table
query = '''SELECT t.name as TableName, ColumnName = col.name, ty.name
	FROM sys.columns col
	INNER JOIN sys.tables as t
		ON col.object_id = t.object_id 
	left join sys.types as ty
		on col.user_type_id = ty.user_type_id
	WHERE  t.name like 'Click%' 
ORDER BY t.name, col.name '''

cur.execute(query)
results = cur.fetchall()

# results field order
# tableName, colName, colDataType 
tableList = []
colList = []
colAttrList = []
for result in results:
    tableList.append(result[0])
    colList.append(result[1])
    colAttrList.append(('Column Data Type', result[2]))
    
create_table.ingest_table(communityName, domainName, None, tableList, None, session, parentCommunityName)
create_column.ingest_column(communityName, domainName, None, tableList, colList, colAttrList, session, parentCommunityName)