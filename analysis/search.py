#!/usr/bin/env python
#import pandas
import sys
import psycopg2
#from sqlalchemy import create_engine
from geo import states
from geo.geolocation import GeoLocation

class SearchLocation:  
  'Provides search functions for database queries'
  def __init__(self, Connection):
    self.__Connection = Connection
    self.__1998_linkage_query = "SELECT cmte_id FROM committee_master WHERE cand_id='%s'"
    self.__oth_linkage_query = "SELECT cmte_id FROM candidate_linkage WHERE cand_id='%s'"
    self.__zipcode_query = "SELECT state, latitude, longitude FROM zipcodes WHERE zip LIKE'%s%%';"
    self.__city_state_query = "SELECT state FROM zipcodes WHERE LOWER(primary_city) LIKE LOWER('%%%s%%');"
    self.__zipcodes_query = "SELECT zip FROM zipcodes WHERE latitude BETWEEN '%s' AND '%s'" + \
                            "AND longitude BETWEEN '%s' AND '%s' and state='%s';"
    self.__cand_zipcodes_query = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation," + \
                                 "cand_city, cand_st FROM candidate_master WHERE cand_zip in %s" + \
				 "ORDER BY cand_name OR cand_id LIKE '__%s%%';"
    self.__state_title_query = "SELECT cand_name, cand_id, cand_pty_affiliation, cand_city," + \
                               "cand_st FROM candidate_master WHERE %s LIKE UPPER('%%%s%%')" + \
			       "and %s LIKE UPPER('%%%s%%') OR cand_id LIKE '__%s%%';"
    self.__city_state_abbr_query = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation, cand_city," +\
                              "cand_st FROM candidate_master WHERE %s LIKE UPPER('%%%s%%') OR cand_id LIKE '__%s%%';"
    self.__first_last_name_query = "SELECT cand_name, cand_id, cand_pty_affiliation, cand_city," + \
                                   "cand_st FROM candidate_master WHERE cand_name LIKE UPPER('%%%s%%')" + \
				   "AND cand_name LIKE UPPER('%%%s%%');"
    self.__name_query = "SELECT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st FROM candidate_master WHERE cand_name LIKE UPPER('%%%s%%');"
    
  def __get_candidate_committees__(self, cands):
    cand_comms = {}
    for candidate in cands:
      if self.__Connection.year <= 1998:
	__linkage_query = self.__1998_linkage_query % candidate[1] 
      else:
	__linkage_query =  self.__oth_linkage_query % candidate[1]
      cand_comms[candidate[0]] = {"cand_id": candidate[1], "comm_ids":[]}
      self.__Connection.fec_cur.execute(__linkage_query)
      committee_ids = self.__Connection.fec_cur.fetchall()
      for committee_id in committee_ids:
        cand_comms[candidate[0]]["comm_ids"].append(committee_id[0])
    return cand_comms   
	  
  def search_names_by_zip(self, parameters):
    'Search by zipcode'
    try:
      zipcode = parameters['zipcode']
      distance = parameters['distance']
      unit = parameters['unit']
    except KeyError:
      raise KeyError("Please define zipcode, distance, and unit")
    
    if unit == "miles":
      distance = distance/0.62137
      
    zipcode_stmt = self.__zipcode_query % zipcode
    self.__Connection.geo_cur.execute(zipcode_stmt)
    state, lat, lon = self.__Connection.geo_cur.fetchone()
    loc = GeoLocation.from_degrees(lat, lon)
    SW_loc, NE_loc = loc.bounding_locations(distance)
    __zipcodes_stmt = self.__zipcodes_query % (SW_loc.deg_lat, NE_loc.deg_lat, SW_loc.deg_lon, NE_loc.deg_lon, state)
    self.__Connection.geo_cur.execute(__zipcodes_stmt)
    zipcodes = self.__Connection.geo_cur.fetchall()
    __zipcodes = []
    
    for __zipcode in zipcodes:
      __zipcodes.append(__zipcode[0].split(".")[0])
    __candidates_query = self.__Connection.fec_cur.mogrify(self.__cand_zipcodes_query, (tuple(__zipcodes), state,))
    self.__Connection.fec_cur.execute(__candidates_query)
    candidates = self.__Connection.fec_cur.fetchall()
    
    candidates_committees = self.get_candidate_committees__(candidates)
    
    return candidates, candidates_committees
  
  def search_by_city_state(self, parameters):
    'Search by city, state, or city and state'
    st_key = 'cand_st'
    city_key = 'cand_city'
    
    try:
      st_query = parameters[st_key].upper()
      city_query = parameters[city_key].upper()
    except KeyError:
      raise KeyError("Please define search parameter")
    
    if st_query and city_query:
      for state in states.states_titles:
	if state['name'] == st_query:
	  st_query = state['abbreviation'].upper()
      __state_query_stmt = __state_title_query % (city_key, city_query, st_key, st_query, st_query)
      
    elif st_query and not city_query:
      for state in states.states_titles:
	if state['name'] == search_query:
	  st_query = state['abbreviation'].upper()
      __state_query_stmt = self.__city_state_abbr_query % (st_key, st_query, st_query)
      
    elif not st_query and city_query:
      __city_state_query = self.__city_state_query % city_query
      self.__Connection.geo_cur.execute(__city_state_query)
      state = self.__Connection.geo_cur.fetchone()
      __state_query_stmt = self.__city_state_abbr_query % (city_key, city_query, state) 
      
    elif not st_query and not city_query:
      return False, False
      
    self.__Connection.fec_cur.execute(__state_query_stmt)
    candidates = self.__Connection.fec_cur.fetchall()
    
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
        __query_by_name = self.__first_last_name_query % tuple(__temp__)
        self.__Connection.__fec_cur.execute(__query_by_name)
        candidates = self.__Connection.fec_cur.fetchall()
        if len(candidates) < 1:
	  __query_by_name = self.__name_query % __temp__[1].strip(',')
	  self.__Connection.__fec_cur.execute(__query_by_name)
	  candidates = self.__Connection.fec_cur.fetchall()
    else:
      query_by_name = self.__name_query % name
      self.__Connection.fec_cur.execute(query_by_name)
      candidates = self.__Connection.fec_cur.fetchall()
    candidates_committees = self.__get_candidate_committees__(candidates)
    return candidates, candidates_committees
  