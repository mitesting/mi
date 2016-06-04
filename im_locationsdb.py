#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import MySQLdb
import sys, traceback
import pandas as pd

"""
this script connects to the locations database. it includes functions to create its 3 tables: cities, countries, regions
if you need to create a new database (say, named locations), remove ", db='locations'" from MySQLdb.connect
and add the following line before returning cursor:
cursor.execute("CREATE DATABASE IF NOT EXISTS locations")
you can then use the new database, adding , ", db='locations'" at the end of MySQLdb.connect or adding
cursor.execute("USE locations") # before returning cursor or in main()
"""

def connect_db():
    try:
        env = os.getenv('SERVER_SOFTWARE')
        if (env and env.startswith('Google App Engine/')):
          # Connecting from App Engine
          db = MySQLdb.connect(
            unix_socket='/cloudsql/*****',
            user='root',
            password='*****', db='locations')
        else:
          # Connecting from an external network.Make sure your network is whitelisted
          db = MySQLdb.connect(
            host='***.***.***.***',
            port=3306,
            user='root',
            passwd='*****', db='locations')
        cursor = db.cursor()
        return db, cursor

    except MySQLdb.Error, msg:
        print msg, traceback.format_exc()
        sys.exit(1)

def countries(db, cursor):
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS countries
        (id INT PRIMARY KEY, alpha2 VARCHAN(20), alpha3 VARCHAN(20), name VARCHAN(70), targetable INT(2) NOT NULL)""")
        ##### add data to countries
        df = pd.read_csv('countries.csv')
        df = df.astype(object).where(pd.notnull(df), None)
        cid = df.id
        alpha2 = df.alpha2
        alpha3 = df.alpha3
        cname = df.name
        targetable = df.targetable
        for i in range(len(cid)):
            sql = "INSERT INTO countries (id, alpha2, alpha3, name, targetable) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql, (cid[i], alpha2[i], alpha3[i], cname[i], targetable[i]))
        #####  add data to countries
        db.commit()
        db.close()
    except Exception, msg:
        print msg, traceback.format_exc()
        db.rollback()
        db.close()

def regions(db, cursor):
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS regions
        (id INT PRIMARY KEY, country_id INT, name VARCHAN(70),iso_code VARCHAN(20));""")
        db.commit()
        ##### add data to regions
        df = pd.read_csv('regions.csv')
        df = df.astype(object).where(pd.notnull(df), None)
        rid = df.id
        country_id = df.country_id
        rname = df.name
        iso_code = df.iso_code
        for i in range(len(rid)):
            # print rid[i], country_id[i], rname[i], iso_code[i]
            sql = "INSERT INTO regions (id,country_id,name,iso_code) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql, (rid[i], country_id[i], rname[i], iso_code[i]))
        #####  add data to regions
        db.commit()
        db.close()
    except Exception, msg:
        print msg, traceback.format_exc()
        db.rollback()
        db.close()

def cities(db, cursor):
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS cities
        (id INT PRIMARY KEY, country_id INT, region_id INT, name VARCHAN(70), iso_code VARCHAN(20))""")
        db.commit()
        ###### add data to cities
        for city in open('cities.txt').readlines():
            try:
                region = eval(city)['region_id']
            except KeyError:
                region = None
            print eval(city)['id'], eval(city)['country_id'], region, eval(city)['name'], eval(city)['iso_code']
            sql = "INSERT INTO cities (id, country_id, region_id, name, iso_code) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql, (eval(city)['id'], eval(city)['country_id'], region, eval(city)['name'], eval(city)['iso_code']))
        db.commit()
        cursor.execute(
            """
            UPDATE cities
            SET region_id = NULL
            WHERE region_id NOT IN (
              SELECT id FROM regions);
            """
        )
        cursor.execute("ALTER TABLE cities ADD FOREIGN KEY fk_region(region_id) REFERENCES regions (id);")
        db.commit()
        db.close()
    except Exception, msg:
        print msg, traceback.format_exc()
        db.rollback()
        db.close()

def show_data_types(cursor):
    cursor.execute("SHOW columns FROM countries")
    print [('Countries: ' + 'Field: ' + id[0], 'Type: ' + id[1], 'Null: ' + id[2], 'Key: ' + id[3], 'Default: ' + str(id[4]),
            'Extra: ' + id[5]) for id in cursor.fetchall()]
    cursor.execute("SHOW columns FROM cities")
    print [('Cities: ' + 'Field: ' + id[0], 'Type: ' + id[1], 'Null: ' + id[2], 'Key: ' + id[3], 'Default: ' + str(id[4]),
            'Extra: ' + id[5]) for id in cursor.fetchall()]
    cursor.execute("SHOW columns FROM regions")
    print [('Regions: ' + 'Field: ' + id[0], 'Type: ' + id[1], 'Null: ' + id[2], 'Key: ' + id[3], 'Default: ' + str(id[4]),
            'Extra: ' + id[5]) for id in cursor.fetchall()]

def select(cursor):
    cursor.execute("""select * from countries c, regions r, cities ci where ci.country_id = c.id
        and ci.region_id = r.id and ci.name = 'Paris'""")
    data = cursor.fetchall()
    for row in data:
        print row

def ro_user(db,  cursor):
    try:
        cursor.execute("GRANT SELECT ON locations.* TO '***'@'%' IDENTIFIED BY '**';")
        db.commit()
        db.close()
    except Exception, msg:
        print msg, traceback.format_exc()
        db.rollback()
        db.close()

def main():
    db, cursor = connect_db() # connects to the locations database
    show_data_types(cursor)
    # regions(db, cursor) # creates, insert data types and populates regions table / duplicate values if uncomment & run
    # cities(db, cursor) # creates, insert data types and populates cities table / duplicate values if uncomment & run
    # countries(db, cursor) #creates,ins data types and populates countries table / duplicate values if uncomment & run
    # ro_user(db, cursor) # creates a read-only user
    select(cursor) # allows select statements

if __name__== "__main__":
    main()