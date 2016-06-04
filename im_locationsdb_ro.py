#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import MySQLdb
import sys, traceback
import pandas as pd

"""
read-only version of the im_locationsdb
tables: cities, countries, regions
"""

def connect_db():
    db = MySQLdb.connect(
        host='***.***.***.***',
        port=3306,
        user='****',
        passwd='****', db='locations')
    cursor = db.cursor()
    return db, cursor

def select(cursor):
    cursor.execute("""select * from countries c, regions r, cities ci where ci.country_id = c.id
        and ci.region_id = r.id and ci.name = 'London'""")
    # field_names = [i[0] for i in cursor.description]
    data = cursor.fetchall()
    for row in data:
        # print field_names
        print row

def main():
    db, cursor = connect_db() # connects to the locations database
    select(cursor) # allows select statements
    db.close()

if __name__== "__main__":
    main()