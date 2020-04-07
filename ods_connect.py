import psycopg2
import configparser
import os

config = configparser.ConfigParser(os.environ)
config.read('db.ini')

dbname = config['aurora']['dbname']
host = config['aurora']['host']
port = config['aurora']['port']
user = config['aurora']['user'] 
pwd = config['aurora']['password']

conn=psycopg2.connect(dbname= dbname, host= host, 
    port= port, user= user, password= pwd, sslmode='require')

#cur = conn.cursor()
#
#query = "select * from u_ops.collibra_metadata_1_vw limit 1;"
#cur.execute(query)
#results = cur.fetchall()
#
#print(results)

def getCursor():
    return conn.cursor()
