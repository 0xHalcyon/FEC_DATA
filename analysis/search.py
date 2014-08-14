#!/usr/bin/env python
#import pandas
import sys
import psycopg2
#from sqlalchemy import create_engine
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
    self.__year = str(conn_settings['year'])
    
    self.geo_conn = psycopg2.connect(dbname=self.__db_prefix+self.__geodb, \
                                 user=self.__db_user,
                                 password=self.__db_password,
	                         host=self.__db_host,
			         port=self.__db_port
			         )
    
    self.geo_conn.set_client_encoding("UTF8")
    self.geo_cur = self.geo_conn.cursor()
    
    self.fec_conn = psycopg2.connect(dbname=self.__db_prefix+self.__year, \
                                 user=self.__db_user,
                                 password=self.__db_password,
	                         host=self.__db_host,
			         port=self.__db_port
			         )
    self.fec_conn.set_client_encoding("UTF8")
    self.fec_cur = self.fec_conn.cursor()
    
  def search_names_by_zip(self, parameters):
    #location_engine_stmt = 'postgresql+psycopg2://%s:%s@%s:%s/%s_geozipcodes' % \
    #              (db_user, db_password, db_host, db_port, db_prefix.lower(), str(year))
    #location_engine = create_engine(engine_stmt)
    zipcode = parameters['zipcode']
    distance = parameters['distance']
    unit = parameters['unit']
    if unit == "miles":
      distance = distance/0.62137
    zipcode_stmt = "SELECT state, latitude, longitude FROM zipcodes WHERE zip LIKE'%s%%';" % zipcode
    self.geo_cur.execute(zipcode_stmt)
    state, lat, lon = self.geo_cur.fetchone()
    loc = GeoLocation.from_degrees(lat, lon)
    print loc
    SW_loc, NE_loc = loc.bounding_locations(distance)
    zipcodes_stmt = "SELECT zip FROM zipcodes WHERE latitude BETWEEN '%s' AND '%s' AND longitude BETWEEN '%s' AND '%s' and state='%s';" % \
                     (SW_loc.deg_lat, NE_loc.deg_lat, SW_loc.deg_lon, NE_loc.deg_lon, state)
    self.geo_cur.execute(zipcodes_stmt)
    zipcodes = self.geo_cur.fetchall()
    __zipcodes = []
    for __zipcode in zipcodes:
      __zipcodes.append(__zipcode[0].split(".")[0])
    #print zipcodes
    __temp = "SELECT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE cand_zip in %s ORDER BY cand_name;"
    candidates_query = s.fec_cur.mogrify(__temp, (tuple(__zipcodes),))
    self.fec_cur.execute(candidates_query)
    return self.fec_cur.fetchall()
    

  