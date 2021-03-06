# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 20:33:00 2020

@author: gagrawa3
"""


import requests
import oracle_hr_connect
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
    
# get cursor object for querying Oracle HR
cur = oracle_hr_connect.getCursor()

parentCommunityName = 'Data Warehouse'
communityName = 'PeopleSoft SUP HR'
domainName = 'HR Physical Data Dictionary'

# query to get metadata from table
#query = ''' select col.owner as schema_name,
#               col.table_name,
#               col.column_name,
#               col.DATA_TYPE
#            from dba_tab_columns col
#            inner join dba_tables t on col.owner = t.owner
#                                          and col.table_name = t.table_name
#                                          and t.owner = 'SYSADM'
#                                          and t.table_name like 'PS!_%' ESCAPE '!'
#            order by col.owner, col.table_name, col.column_id '''
   
query = ''' select A.recname,
       C.RECDESCR,
       A.fieldname,
       CASE D.FIELDTYPE WHEN 0 THEN 'Character'
                        WHEN 1 THEN 'Long Character'
                        WHEN 2 THEN 'Number'
                        WHEN 3 THEN 'Signed Number'
                        WHEN 4 THEN 'Date'
                        WHEN 5 THEN 'Time'
                        WHEN 6 THEN 'DateTime'
                        WHEN 8 THEN 'Image/Attachment'
                        WHEN 9 THEN 'Image Reference'
                        ELSE 'Unknown' END AS FIELDTYPE,
       B.LONGNAME AS FIELDDESCR
from PSRECFIELDDB A
join psdbfldlabl B on B.fieldname = A.fieldname and B.DEFAULT_LABEL = 1
join psrecdefn C on A.recname = C.recname
join psdbfield D on D.FIELDNAME = A.FIELDNAME
--where A.recname = 'ACA_EMPLOYEE'
order by A.RECNAME, A.FIELDNUM '''

cur.execute(query)
results = cur.fetchall()

# results field order
# tableName, tableDescr, colName, colType, colDescr 
# no schemaList necessary here
tableList = []
colList = []
tableAttrList = []
colAttrList = []
for result in results:
    tableList.append(result[0])
    colList.append(result[2])
    tableAttrList.append(('Description', result[1]))
    colAttrList.append((('Description', result[4]), ('Column Data Type', result[3])))

create_table.ingest_table(communityName, domainName, None, tableList, tableAttrList, session, parentCommunityName)
create_column.ingest_column(communityName, domainName, None, tableList, colList, colAttrList, session, parentCommunityName)