# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 16:27:01 2020

@author: gagrawa3
"""

import pyodbc
import configparser
import os

config = configparser.ConfigParser(os.environ)
config.read('db.ini')

server = config['sql-server']['server']
dbname = config['sql-server']['dbname']
user = config['sql-server']['user'] 
pwd = config['sql-server']['password']

conn=pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+dbname+';UID='+user+';PWD='+ pwd)

def getCursor():
    return conn.cursor()
