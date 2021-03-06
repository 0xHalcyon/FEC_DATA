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
    import random
    import string
    import os
    self.save_point_length = 25
    self.random = random
    self.random.seed = os.urandom(1024)
    self.random_chars = string.ascii_letters + string.digits
    self.__Connection = Connection
    self.start_year = Connection.start_year
    self.end_year = Connection.end_year
    self.__1998_linkage_query = "SELECT cmte_id FROM committee_master_%s WHERE cand_id='%s'"
    self.__oth_linkage_query = "SELECT cmte_id FROM candidate_linkage_%s WHERE cand_id='%s'"
    self.__zipcode_query = "SELECT state, latitude, longitude FROM zipcodes WHERE zip LIKE'%s%%';"
    self.__city_state_query = "SELECT state FROM zipcodes WHERE LOWER(primary_city) LIKE LOWER('%%%s%%');"
    self.__zipcodes_query = "SELECT MIN(zip), MAX(zip) FROM zipcodes WHERE latitude BETWEEN '%s' AND '%s'" + \
                            "AND longitude BETWEEN '%s' AND '%s' and state='%s';"
    #self.__cand_zipcodes_query = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation," + \
                                 #"cand_city, cand_st FROM candidate_master_{0} WHERE cand_zip IN %s " + \
				 #"ORDER BY cand_name;"
    self.__cand_zipcodes_query = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation, " + \
                                 "cand_city, cand_st FROM candidate_master_{0} WHERE cand_zip >= '%s'" + \
				 " and cand_zip <= '%s';"
    self.__state_query = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation, cand_city," +\
                              "cand_st FROM candidate_master_{0} WHERE %s LIKE UPPER('%%%s%%') OR cand_id LIKE '__%s%%' ESCAPE ' ';"
    self.__city_state_query = "SELECT DISTINCT cand_name, cand_id, cand_pty_affiliation, cand_city," +\
                              "cand_st FROM candidate_master_{0} WHERE %s LIKE UPPER('%%%s%%') AND %s LIKE UPPER('%%%s%%');"    
    self.__first_last_name_query = "SELECT cand_name, cand_id, cand_pty_affiliation, cand_city," + \
                                   "cand_st FROM candidate_master_{0} WHERE cand_name LIKE UPPER('%%%s%%') " + \
				   "AND cand_name LIKE UPPER('%%%s%%');"
    self.__name_query = "SELECT cand_name, cand_id, cand_pty_affiliation, cand_city, cand_st " + \
                        "FROM candidate_master_%s WHERE cand_name LIKE UPPER('%%%s%%');"
  def __remove_duplicates__(self, seq):
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if not (x in seen or seen_add(x))]
  
  def __get_candidate_committees__(self, cands):
    cand_comms = {}
    for candidate in cands:
      for year in range(self.start_year, self.end_year, 2):
        if year <= 1998:
	  __linkage_query = self.__1998_linkage_query % (year, candidate[1])
        else:
	  __linkage_query =  self.__oth_linkage_query % (year, candidate[1])
        cand_comms[candidate[1]] = {"cand_name": candidate[0], "comm_ids":[]}
        try:
	  self.__Connection.cur.execute("BEGIN;")
	  save_point = ''.join(self.random.choice(self.random_chars) for i in range(self.save_point_length))
	  self.__Connection.cur.execute("SAVEPOINT save_%s;" % save_point)
          self.__Connection.cur.execute(__linkage_query)
          committee_ids = self.__Connection.cur.fetchall()
        except (psycopg2.ProgrammingError, psycopg2.InternalError):
          self.__Connection.cur.execute("ROLLBACK TO SAVEPOINT save_%s;" % save_point)
          continue
	else:
	  self.__Connection.cur.execute("RELEASE SAVEPOINT save_%s;" % save_point)
        for committee_id in committee_ids:
          cand_comms[candidate[1]]["comm_ids"].append(committee_id[0])
    #cand_comms = self.__remove_duplicates__(cand_comms)
    print cand_comms
    return cand_comms   
	  
  def search_names_by_zip(self, parameters):
    """Search by zipcode
    parameters = {'zipcode':zip, 'distance':distance, 'unit':measure_unit}
    'unit' must be miles or kilometers
    'distance' is any arbitrary integer value
    'zipcode' is integer
    """
    try:
      zipcode = int(parameters['zipcode'])
      distance = parameters['distance']
      unit = parameters['unit']
    except KeyError:
      raise KeyError("Please define zipcode, distance, and unit")
    except (ValueError, TypeError):
      raise ValueError("Invalid Zipcode")
    
    if unit == "miles":
      distance = distance/0.62137
      
    zipcode_stmt = self.__zipcode_query % zipcode
    self.__Connection.cur.execute(zipcode_stmt)
    results = self.__Connection.cur.fetchone()
    if not results:
      return False, False
    state, lat, lon = results
    loc = GeoLocation.from_degrees(lat, lon)
    SW_loc, NE_loc = loc.bounding_locations(distance)
    __zipcodes_stmt = self.__zipcodes_query % (SW_loc.deg_lat, NE_loc.deg_lat, SW_loc.deg_lon, NE_loc.deg_lon, state)
    self.__Connection.cur.execute(__zipcodes_stmt)
    zipcodes = self.__Connection.cur.fetchone()
    print zipcodes
    __zipcodes = []
    candidates = []
    for __zipcode in zipcodes:
      print __zipcode
      __zipcodes.append(int(__zipcode.split(".")[0]))
    print __zipcodes
    for year in range(self.start_year, self.end_year, 2):
      print state
      __candidates_query = self.__cand_zipcodes_query.format(year)
      print __candidates_query
      #__candidates_query = self.__Connection.cur.mogrify(__candidates_query, (tuple(__zipcodes),))
      __candidates_query = __candidates_query % (__zipcodes[0], __zipcodes[1])
      print __candidates_query
      try:
	self.__Connection.cur.execute("BEGIN;")
	save_point = ''.join(self.random.choice(self.random_chars) for i in range(self.save_point_length))
	self.__Connection.cur.execute("SAVEPOINT save_%s;" % save_point)
        self.__Connection.cur.execute(__candidates_query)
        candidates += self.__Connection.cur.fetchall()
      except (psycopg2.ProgrammingError, psycopg2.InternalError):
        self.__Connection.cur.execute("ROLLBACK TO SAVEPOINT save_%s;" % save_point)
        continue
      else:
	self.__Connection.cur.execute("RELEASE SAVEPOINT save_%s;" % save_point)
    candidates = self.__remove_duplicates__(candidates)
    candidates_committees = self.__get_candidate_committees__(candidates)
    
    return candidates, candidates_committees
  
  def search_by_city_state(self, parameters):
    """Search by city, state, or city and state
    parameters = {'cand_st':state, 'cand_city': city}
    """
    st_key = 'cand_st'
    city_key = 'cand_city'
    
    try:
      st_query = parameters[st_key].upper()
      city_query = parameters[city_key].upper()
    except KeyError:
      raise KeyError("Please define search parameter")
    
    if st_query and city_query:
      print "State and city search"
      print st_query, city_query
      for state in states.states_titles:
	if state['name'].lower() == st_query.lower():
	  st_query = state['abbreviation'].upper()
      __state_query_stmt = self.__city_state_query % (city_key, city_query, st_key, st_query)
      
    elif st_query and not city_query:
      print "State NOT city search"
      print st_query, city_query
      for state in states.states_titles:
	if state['name'].lower() == st_query.lower():
	  st_query = state['abbreviation'].upper()
      __state_query_stmt = self.__state_query % (st_key, st_query, st_query)
      
    elif not st_query and not city_query:
      print "Not sure what's up here"
      print st_query, city_query
      return False, False
    
    candidates = []
    for year in range(self.start_year, self.end_year, 2):   
      print __state_query_stmt
      try:
	self.__Connection.cur.execute("BEGIN;")
	save_point = ''.join(self.random.choice(self.random_chars) for i in range(self.save_point_length))
	self.__Connection.cur.execute("SAVEPOINT save_%s;" % save_point)
        self.__Connection.cur.execute(__state_query_stmt.format(str(year)))        
        candidates += self.__Connection.cur.fetchall()
      except (psycopg2.ProgrammingError, psycopg2.InternalError):
	self.__Connection.cur.execute("ROLLBACK TO save_%s;" % save_point)
	continue
      else:
	self.__Connection.cur.execute("RELEASE SAVEPOINT save_%s;" % save_point)      
    candidates = self.__remove_duplicates__(candidates)
    candidates_committees= self.__get_candidate_committees__(candidates)
      # return ([(name, cand_id, cand_pty_affiliation, cand_city, cand_st), ...], {cand_name : {cand_id: 'cand_id', comm_ids: [cmte_id]}}
    return candidates, candidates_committees    

  def search_by_name(self, parameters):
    """Search by name
    parameters = {'name':name}
    """
    try:
      name = parameters['name'].strip()
    except KeyError:
      raise KeyError("Please define name")
    candidates = []
    for year in range(self.start_year, self.end_year, 2):
      if len(name.split(" ")) > 1 or "," in name:
	__temp__ = name.replace(",", "")
        __temp__ = name.split(" ")
        if len(__temp__) > 0:
          query_by_name = self.__first_last_name_query % (__temp__[0], __temp__[1])
          print query_by_name.format(year)
          try:
	    self.__Connection.cur.execute("BEGIN;")
	    save_point = ''.join(self.random.choice(self.random_chars) for i in range(self.save_point_length))
	    self.__Connection.cur.execute("SAVEPOINT save_%s;" % save_point)	    
            self.__Connection.cur.execute(query_by_name.format(year))
            candidates = self.__Connection.cur.fetchall()
          except (psycopg2.ProgrammingError, psycopg2.InternalError):
	    self.__Connection.cur.execute("ROLLBACK TO save_%s;" % save_point)
	    continue
	  else:
	    self.__Connection.cur.execute("RELEASE SAVEPOINT save_%s;" % save_point)
          if len(candidates) < 1:
	    query_by_name = self.__name_query % (year, __temp__[1])
	    print query_by_name
	    self.__Connection.cur.execute(query_by_name)
	    candidates = self.__Connection.cur.fetchall()
      else:
        query_by_name = self.__name_query % (year, name)
        print query_by_name
        self.__Connection.cur.execute(query_by_name)
        candidates = self.__Connection.cur.fetchall()
        
    candidates = self.__remove_duplicates__(candidates)
    candidates_committees = self.__get_candidate_committees__(candidates)
    return candidates, candidates_committees
  