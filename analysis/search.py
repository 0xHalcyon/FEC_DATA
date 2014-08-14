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
    self.__year = conn_settings['year']
    try:
      self.geo_conn = psycopg2.connect(dbname=self.__db_prefix+self.__geodb, \
                                       user=self.__db_user,
                                       password=self.__db_password,
	                               host=self.__db_host,
			               port=self.__db_port
			               )
    
      self.geo_conn.set_client_encoding("UTF8")
      self.geo_cur = self.geo_conn.cursor()
    
      self.fec_conn = psycopg2.connect(dbname=self.__db_prefix+str(self.__year), \
                                       user=self.__db_user,
                                       password=self.__db_password,
	                               host=self.__db_host,
			               port=self.__db_port
			               )
      self.fec_conn.set_client_encoding("UTF8")
      self.fec_cur = self.fec_conn.cursor()
    except psycopg2.Error:
      raise Exception("Did you define database parameters in config?")
    
  def __del__(self):
    self.fec_cur.close()
    self.geo_cur.close()
    self.fec_conn.close()
    self.geo_conn.close()
    
  def search_names_by_zip(self, parameters):
    try:
      zipcode = parameters['zipcode']
      distance = parameters['distance']
      unit = parameters['unit']
    except KeyError:
      raise KeyError("Please define zipcode, distance, and unit")
    
    if unit == "miles":
      distance = distance/0.62137
      
    zipcode_stmt = "SELECT state, latitude, longitude FROM zipcodes WHERE zip LIKE'%s%%';" % zipcode
    self.geo_cur.execute(zipcode_stmt)
    state, lat, lon = self.geo_cur.fetchone()
    loc = GeoLocation.from_degrees(lat, lon)
    SW_loc, NE_loc = loc.bounding_locations(distance)
    zipcodes_stmt = "SELECT zip FROM zipcodes WHERE latitude BETWEEN '%s' AND '%s' AND longitude BETWEEN '%s' AND '%s' and state='%s';" % \
                     (SW_loc.deg_lat, NE_loc.deg_lat, SW_loc.deg_lon, NE_loc.deg_lon, state)
    self.geo_cur.execute(zipcodes_stmt)
    zipcodes = self.geo_cur.fetchall()
    __zipcodes = []
    
    for __zipcode in zipcodes:
      __zipcodes.append(__zipcode[0].split(".")[0])
    __temp = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE cand_zip in %s ORDER BY cand_name;"
    candidates_query = self.fec_cur.mogrify(__temp, (tuple(__zipcodes),))
    self.fec_cur.execute(candidates_query)
    candidates = self.fec_cur.fetchall()
    
    if self.__year <= 1998:
      candidates_committees = {}
      for candidate in candidates:
	linkage_query = "SELECT cmte_id FROM committee_master WHERE cand_id='%s'" % candidate[1]
        candidates_committees[candidate[0]] = {"cand_id": candidate[1], "comm_ids":[]}
        self.fec_cur.execute(linkage_query)
        committee_ids = self.fec_cur.fetchall()
        for committee_id in committee_ids:
	  candidates_committees[candidate[0]]["comm_ids"].append(committee_id[0])
      return candidates, candidates_committees
    
    if self.__year > 1998:
      
      candidates_committees = {}    
      for candidate in candidates:
        linkage_query = "SELECT cmte_id FROM candidate_linkage WHERE cand_id='%s'" % candidate[1]
        candidates_committees[candidate[0]] = {"cand_id": candidate[1], "comm_ids":[]}
        self.fec_cur.execute(linkage_query)
        committee_ids = self.fec_cur.fetchall()
        for committee_id in committee_ids:
	  candidates_committees[candidate[0]]["comm_ids"].append(committee_id[0])
      # return ([(name, cand_id, cand_pty_affiliation, cand_city, cand_st), ...], {cand_name : {cand_id: 'cand_id', comm_ids: [cmte_id]}}
      return candidates, candidates_committees
  
  def search_by_other(self, parameters):
    try:
      search_key = parameters['search']
      search_query = parameters['query']
    except KeyError:
      raise KeyError("Please define search parameter")
    if ',' in search_query:
      city_key = 'cand_city'
      st_key = 'cand_st'
      city = search_query.split(',')[0]
      st = search_query.split(',')[1]
      query_stmt = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE %s LIKE UPPER('%%%s%%') and %s LIKE UPPER('%%%s%%');" % \
	            (city_key, city, st_key, st)
    else:
      query_stmt = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE %s LIKE UPPER('%%%s%%');" % (search_key, search_query)
    self.fec_cur.execute(query_stmt)
    candidates = self.fec_cur.fetchall()
    
    if self.__year <= 1998:
      candidates_committees = {}
      for candidate in candidates:
	linkage_query = "SELECT cmte_id FROM committee_master WHERE cand_id='%s'" % candidate[1]
        candidates_committees[candidate[0]] = {"cand_id": candidate[1], "comm_ids":[]}
        self.fec_cur.execute(linkage_query)
        committee_ids = self.fec_cur.fetchall()
        for committee_id in committee_ids:
	  candidates_committees[candidate[0]]["comm_ids"].append(committee_id[0])
      return candidates, candidates_committees
    
    if self.__year > 1998:
      
      candidates_committees = {}    
      for candidate in candidates:
        linkage_query = "SELECT cmte_id FROM candidate_linkage WHERE cand_id='%s'" % candidate[1]
        candidates_committees[candidate[0]] = {"cand_id": candidate[1], "comm_ids":[]}
        self.fec_cur.execute(linkage_query)
        committee_ids = self.fec_cur.fetchall()
        for committee_id in committee_ids:
	  candidates_committees[candidate[0]]["comm_ids"].append(committee_id[0])
      # return ([(name, cand_id, cand_pty_affiliation, cand_city, cand_st), ...], {cand_name : {cand_id: 'cand_id', comm_ids: [cmte_id]}}
      return candidates, candidates_committees    

  def search_by_city(self, parameters):
    pass

  