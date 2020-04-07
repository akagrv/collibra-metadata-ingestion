# -*- coding: utf-8 -*-
"""
Created on Tue Feb 12 11:37:15 2020

@author: gagrawa3
"""

import requests
import ods_connect
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
    
# get cursor object for querying ODS
cur = ods_connect.getCursor()

parentCommunityName = 'ODS'
communityName = 'Aurora'
domainName = 'Aurora Physical Data Dictionary'

# query to get metadata from table
query = "select schemaname, tablename, columnname, data_type from u_ops.collibra_metadata_1_vw;"

cur.execute(query)
results = cur.fetchall()

schemaList = [result[0] for result in results]
tableList = [result[1] for result in results]

create_schema.ingest_schema(communityName, domainName, schemaList, session, parentCommunityName)
create_table.ingest_table(communityName, domainName, schemaList, tableList, session, parentCommunityName)
create_column.ingest_column(communityName, domainName, results, session, parentCommunityName)