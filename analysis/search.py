#!/usr/bin/env python
#import pandas
import sys
import psycopg2
#from sqlalchemy import create_engine
from geo import states
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
      self.__geo_conn = psycopg2.connect(dbname=self.__db_prefix+self.__geodb, \
                                       user=self.__db_user,
                                       password=self.__db_password,
	                               host=self.__db_host,
			               port=self.__db_port
			               )
    
      self.__geo_conn.set_client_encoding("UTF8")
      self.__geo_cur = self.__geo_conn.cursor()
    
      self.__fec_conn = psycopg2.connect(dbname=self.__db_prefix+str(self.__year), \
                                       user=self.__db_user,
                                       password=self.__db_password,
	                               host=self.__db_host,
			               port=self.__db_port
			               )
      self.__fec_conn.set_client_encoding("UTF8")
      self.__fec_cur = self.__fec_conn.cursor()
    except psycopg2.Error:
      raise Exception("Did you define database parameters in config?")
    
  def __del__(self):
    self.__fec_cur.close()
    self.__geo_cur.close()
    self.__fec_conn.close()
    self.__geo_conn.close()
    
  def __get_candidate_committees__(self, cands):
    
    if self.__year <= 1998:
      cand_comms = {}
      for candidate in cands:
	linkage_query = "SELECT cmte_id FROM committee_master WHERE cand_id='%s'" % candidate[1]
        cand_comms[candidate[0]] = {"cand_id": candidate[1], "comm_ids":[]}
        Connect.__fec_cur.execute(linkage_query)
        committee_ids = Connect.__fec_cur.fetchall()
        for committee_id in committee_ids:
	  cand_comms[candidate[0]]["comm_ids"].append(committee_id[0])
      return cand_comms
    
    if self.__year > 1998:
      cand_comms = {}    
      for candidate in cands:
        linkage_query = "SELECT cmte_id FROM candidate_linkage WHERE cand_id='%s'" % candidate[1]
        cand_comms[candidate[0]] = {"cand_id": candidate[1], "comm_ids":[]}
        Connect.__fec_cur.execute(linkage_query)
        committee_ids = Connect.__fec_cur.fetchall()
        for committee_id in committee_ids:
	  cand_comms[candidate[0]]["comm_ids"].append(committee_id[0])
      return cand_comms
	  
  def search_names_by_zip(self, Connect, parameters):
    'Search by zipcode'
    try:
      zipcode = parameters['zipcode']
      distance = parameters['distance']
      unit = parameters['unit']
    except KeyError:
      raise KeyError("Please define zipcode, distance, and unit")
    
    if unit == "miles":
      distance = distance/0.62137
      
    zipcode_stmt = "SELECT state, latitude, longitude FROM zipcodes WHERE zip LIKE'%s%%';" % zipcode
    Connect.__geo_cur.execute(zipcode_stmt)
    state, lat, lon = Connect.__geo_cur.fetchone()
    loc = GeoLocation.from_degrees(lat, lon)
    SW_loc, NE_loc = loc.bounding_locations(distance)
    zipcodes_stmt = "SELECT zip FROM zipcodes WHERE latitude BETWEEN '%s' AND '%s' AND longitude BETWEEN '%s' AND '%s' and state='%s';" % \
                     (SW_loc.deg_lat, NE_loc.deg_lat, SW_loc.deg_lon, NE_loc.deg_lon, state)
    Connect.__geo_cur.execute(zipcodes_stmt)
    zipcodes = Connect.__geo_cur.fetchall()
    __zipcodes = []
    
    for __zipcode in zipcodes:
      __zipcodes.append(__zipcode[0].split(".")[0])
    __temp = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE cand_zip in %s ORDER BY cand_name;"
    candidates_query = Connect.__fec_cur.mogrify(__temp, (tuple(__zipcodes),))
    Connect.__fec_cur.execute(candidates_query)
    candidates = Connect.__fec_cur.fetchall()
    
    candidates_committees = self.__get_candidate_committees__(candidates)
    
    return candidates, candidates_committees
  
  def search_by_other(self, parameters):
    'Search by city, state, or city and state'
    try:
      search_key = parameters['search']
      search_query = parameters['query']
    except KeyError:
      raise KeyError("Please define search parameter")
    if ',' in search_query:
      city_key = 'cand_city'
      st_key = 'cand_st'
      city = search_query.split(', ')[0]
      st = search_query.split(', ')[1]
      for state in states.states_titles:
	if state['name'] == st:
	  st = state['abbreviation']
      query_stmt = "SELECT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE %s LIKE UPPER('%%%s%%') and %s LIKE UPPER('%%%s%%');" % \
	            (city_key, city, st_key, st)
    else:
      for state in states.states_titles:
	if state['name'] == search_query:
	  search_query = state['abbreviation']
      query_stmt = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE %s LIKE UPPER('%%%s%%');" % (search_key, search_query)
    Connect.__fec_cur.execute(query_stmt)
    candidates = Connect.__fec_cur.fetchall()
    
    candidates_committees= self.__get_candidate_committees__(candidates)
      # return ([(name, cand_id, cand_pty_affiliation, cand_city, cand_st), ...], {cand_name : {cand_id: 'cand_id', comm_ids: [cmte_id]}}
    return candidates, candidates_committees    

  def search_by_name(self, parameters):
    'Search by name'
    try:
      name = parameters['name'].strip()
    except KeyError:
      raise KeyError("Please define name")
    if len(name.split(" ")) > 1 or "," in name:
      __temp__ = name.split(" ")
      if len(__temp__) > 0:
        query_by_name = "SELECT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE cand_name LIKE UPPER('%%%s%%') AND cand_name LIKE UPPER('%%%s%%');" % tuple(__temp__)
        Connect.__fec_cur.execute(query_by_name)
        candidates = Connect.__fec_cur.fetchall()
        if len(candidates) < 1:
	  query_by_name = "SELECT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE cand_name LIKE UPPER('%%%s%%');" % __temp__[1].strip(',')
	  Connect.__fec_cur.execute(query_by_name)
	  candidates = Connect.__fec_cur.fetchall()
    else:
      query_by_name = "SELECT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE cand_name LIKE UPPER('%%%s%%');" % name
      Connect.__fec_cur.execute(query_by_name)
      candidates = Connect.__fec_cur.fetchall()
    candidates_committees = self.__get_candidate_committees__(candidates)
    return candidates, candidates_committees
  