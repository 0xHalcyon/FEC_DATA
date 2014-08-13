#!/usr/bin/env python

import psycopg2
import json
import sqlalchemy
from time import sleep
from sqlalchemy import create_engine

def gengeodb(cwd, db_prefix, db_user, db_password, db_host, db_port):
  zipcodes = json.load(open(cwd+"/geo/zipcodes.json"))
  try:
    conn = psycopg2.connect(dbname=db_prefix.lower()+"geozipcodes",
			    user=db_user,
			    password=db_password,
			    host=db_host,
			    port=db_port
			    )
    cur = conn.cursor()
    
  except psycopg2.OperationalError as e:
    try:
      print "Database %sgeozipcodes does not exist yet, creating now" % db_prefix.lower()
      engine_stmt = 'postgresql+psycopg2://%s:%s@%s:%s/template1' % \
                    (db_user, db_password, db_host, db_port)
      engine = create_engine(engine_stmt)
      eng_conn = engine.connect()
      eng_conn.connection.connection.set_isolation_level(0)
      create_db_stmt = "CREATE DATABASE %sgeozipcodes" % db_prefix.lower()
      eng_conn.execute(create_db_stmt)
      eng_conn.connection.connection.set_isolation_level(1)
      eng_conn.close()
      engine.dispose()
      sleep(1)
    except sqlalchemy.exc.ProgrammingError as e:
      print "Error:%s\nDid you modify your configuration file and run make createuser?" % e
      os.sys.exit(1)
  conn = psycopg2.connect(dbname=db_prefix.lower()+"geozipcodes",
 	                  user=db_user,
			  password=db_password,
			  host=db_host,
			  port=db_port
			  )
  cur = conn.cursor()
    
  try:
    cur.execute("CREATE TABLE zipcodes ( \
                 zip VARCHAR(10) UNIQUE NOT NULL, \
	         type TEXT, \
	         primary_city TEXT, \
	         acceptable_cities TEXT, \
	         unacceptable_cities TEXT, \
	         state VARCHAR(5), \
	         county TEXT, \
	         timezone TEXT, \
	         area_codes TEXT, \
	         latitude REAL, \
	         logitude REAL, \
	         world_region TEXT, \
	         country TEXT, \
	         decommissioned TEXT, \
	         estimated_population TEXT, \
	         notes TEXT)")
  except psycopg2.Error as e:
    print "We could not create the table, %s" % e
    os.sys.exit(1)
  for zipcode in zipcodes:
	
    stmt = "INSERT INTO zipcodes (zip, type, primary_city, acceptable_cities, unacceptable_cities, \
                               state, county, timezone, area_codes, latitude, longitude, \
                               world_region, country, decommissioned, estimated_population, notes) \
                               VALUES (%s, %s, %s, %s, %s, %s,\
                               %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);" % \
                               tuple(zipcode)
    print stmt
    cur.execute(stmt)
  conn.commit()
  cur.close()
  conn.close()