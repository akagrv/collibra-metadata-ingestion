import psycopg2
import configparser
import os

config = configparser.ConfigParser(os.environ)
config.read('db.ini')

dbname = config['redshift']['dbname']
host = config['redshift']['host']
port = config['redshift']['port']
user = config['redshift']['user'] 
pwd = config['redshift']['password']

conn=psycopg2.connect(dbname= dbname, host= host, 
    port= port, user= user, password= pwd, sslmode='require')

def getCursor():
    return conn.cursor()