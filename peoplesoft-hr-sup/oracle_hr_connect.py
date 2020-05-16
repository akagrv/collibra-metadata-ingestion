# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 20:30:46 2020

@author: gagrawa3
"""

import cx_Oracle
import configparser
import os

config = configparser.ConfigParser(os.environ)
config.read('db.ini')

dbname = config['oracle-hr']['dbname']
host = config['oracle-hr']['host']
port = config['oracle-hr']['port']
user = config['oracle-hr']['user'] 
pwd = config['oracle-hr']['password']

conn_string = host + "/" + dbname 

conn = cx_Oracle.connect(user, pwd, conn_string)

def getCursor():
    return conn.cursor()