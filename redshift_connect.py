import psycopg2
import configparser

config = configparser.RawConfigParser()
config.read('db.ini')

dbname = config['redshift']['dbname']
host = config['redshift']['host']
port = config['redshift']['port']
user = config['redshift']['user'] 
pwd = config['redshift']['password']

conn=psycopg2.connect(dbname= dbname, host= host, 
    port= port, user= user, password= pwd)

def getCursor():
    return conn.cursor()