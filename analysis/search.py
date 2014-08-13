#!/usr/bin/env python
import pandas
import sys
from sqlalchemy import create_engine

class Search:  
  'Provides search functions for database queries'
  def __init__(self, conn_settings):
    self.__db_prefix = conn_settings['db_prefix']
    self.__db_user = conn_settings['db_user']
    self.__db_password = conn_settings['db_password']
    self.__db_host = conn_settings['db_host']
    self.__db_port = conn_settings['db_port']
  
  def search_names_geo(self, parameters):
    location_engine_stmt = 'postgresql+psycopg2://%s:%s@%s:%s/%s_geozipcodes' % \
                  (db_user, db_password, db_host, db_port, db_prefix.lower(), str(year))
    location_engine = create_engine(engine_stmt)
    if year <= 1998:
      candidates_stmt = "SELECT * FROM candidate_master WHERE {0} LIKE '%%{1}%%';".format(field, query.upper())
      names = pandas.read_sql(candidates_stmt, engine)
    
  