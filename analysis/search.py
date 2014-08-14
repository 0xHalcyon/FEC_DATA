#!/usr/bin/env python
import pandas
import sys
import psycopg2
from sqlalchemy import create_engine
from geo.geolocation import GeoLocation

class SearchLocation:  
  'Provides search functions for database queries'
  def __init__(self, conn_settings):
    self.__geodb = "geozipcodes"
    self.__db_prefix = conn_settings['db_prefix']
    self.__db_user = conn_settings['db_user']
    self.__db_password = conn_settings['db_password']
    self.__db_host = conn_settings['db_host']
    self.__db_port = conn_settings['db_port']
    self.conn = psycopg2.connect(dbname=self.__db_prefix+self.__geodb, \
                                 user=self.__db_user,
                                 password=self.__db_password,
	                         host=self.__db_host,
			         port=self.__db_port
			         )
    self.conn.set_client_encoding("UTF8")
    self.cur = self.conn.cursor()
    
  def search_names_by_zip(self, parameters):
    #location_engine_stmt = 'postgresql+psycopg2://%s:%s@%s:%s/%s_geozipcodes' % \
    #              (db_user, db_password, db_host, db_port, db_prefix.lower(), str(year))
    #location_engine = create_engine(engine_stmt)
    zipcode = parameters['zipcode']
    distance = parameters['distance']
    unit = parameters['unit']
    if unit == "miles":
      distance = 
    zipcode_stmt = "SELECT latitude, longitude FROM zipcodes WHERE zip LIKE'%s%%';" % zipcode
    self.cur.execute(zipcode_stmt)
    location = self.cur.fetchall()
    print location
    #loc = GeoLocation.from_degrees(lat, lon)
    #SW_loc, NE_loc = loc.bounding_locations(distance)
    #zipcodes_stmt = "SELECT zip FROM zipcodes WHERE latitude BETWEEN '%s' AND '%s' AND longitude BETWEEN '%s' AND '%s';" % \
    #                 (SW_loc.deg_lat, NW_loc.deg_lat, SW_loc.deg_log, NE_loc.deg_lon)

  