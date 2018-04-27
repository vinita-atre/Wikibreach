#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 27 08:33:10 2018

@author: mayankajha
"""

import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns
color = sns.color_palette()
%matplotlib inline

import time
import mysql.connector
#import MySQLdb as my
from sqlalchemy import create_engine
#from python_mysql_dbconfig import read_db_config


df = pd.read_csv("Privacy_Rights_Clearinghouse-Data-Breaches-Export.csv")

#Splitting the df into separate ones based on the database table
incident_report_df = df[['Date Made Public', 'Source URL', 'Description of incident', 'Information Source']]
victim_df = df[['Type of organization', 'Company']]


incident_report_df.columns = ['reported_date','source_url', 'incident_summary', 'reported_by']
victim_df.columns = ['victim_type', 'victim_name']

#Converting string to datetime format

for index, row in incident_report_df.iterrows():
    datestring = row['reported_date']
    row['reported_date'] = time.strptime(datestring,"%d-%b-%y")
    
for row in incident_report_df:
    print (type(incident_report_df[row][1]))
    
#Replacing all the null values with Missing Values string
incident_report_df = incident_report_df.replace(np.nan, 'Missing Values', regex=True)

#Ingesting incident_report_df to incident report table
engine = mysql.connector.connect(user='root', password='xx', database='wikibreach')
cursor = engine.cursor()

for index, rows in incident_report_df.iterrows():
    insert_stmt = "INSERT INTO incident_report (reported_date, source_url, incident_summary, reported_by) VALUES (%s, %s, %s, %s)"
    data = (rows.reported_date, rows.source_url, rows.incident_summary, rows.reported_by)
    cursor.execute(insert_stmt, data)
    
    engine.commit()

cursor.close()
engine.close()


#Ingesting victim_df to victim table
engine = mysql.connector.connect(user='root', password='xx', database='wikibreach')
cursor = engine.cursor()

for index, rows in victim_df.iterrows():
    insert_stmt = "INSERT INTO victim (victim_type, victim_name) VALUES (%s, %s)"
    data = (rows.victim_type, rows.victim_name)
    cursor.execute(insert_stmt, data)
    
    engine.commit()

cursor.close()
engine.close()