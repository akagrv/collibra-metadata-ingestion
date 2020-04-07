# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 13:58:41 2020

@author: gagrawa3
"""

import cx_Oracle
import configparser
import os

config = configparser.ConfigParser(os.environ)
config.read('db.ini')

dbname = config['oracle']['dbname']
host = config['oracle']['host']
port = config['oracle']['port']
user = config['oracle']['user'] 
pwd = config['oracle']['password']

conn_string = host + "/" + dbname 

conn = cx_Oracle.connect(user, pwd, conn_string)

def getCursor():
    return conn.cursor()