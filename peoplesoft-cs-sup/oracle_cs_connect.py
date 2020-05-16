# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 20:31:56 2020

@author: gagrawa3
"""

import cx_Oracle
import configparser
import os

config = configparser.ConfigParser(os.environ)
config.read('db.ini')

dbname = config['oracle-cs']['dbname']
host = config['oracle-cs']['host']
port = config['oracle-cs']['port']
user = config['oracle-cs']['user'] 
pwd = config['oracle-cs']['password']

conn_string = host + "/" + dbname 

conn = cx_Oracle.connect(user, pwd, conn_string)

def getCursor():
    return conn.cursor()