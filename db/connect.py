#!/usr/bin/env python
import psycopg2
from psycopg2 import pool
class Connection:
  def __init__(self, conn_settings):
    try:
      self.__db_name = conn_settings['db_name']
      self.__db_user = conn_settings['db_user']
      self.__db_password = conn_settings['db_password']
      self.__db_host = conn_settings['db_host']
      self.__db_port = conn_settings['db_port']
      self.start_year = conn_settings['start_year']
      self.end_year = conn_settings['end_year']
      self.max_conns = self.end_year - self.start_year
    except KeyError:
      print "Please define 'db_name', 'db_user', 'db_password', 'db_host', 'db_port', 'start_year', 'end_year'"
      exit(1)
    try:       
      self.conns = pool.ThreadedConnectionPool(0,
					       self.max_conns, 
                                               user=self.__db_user,
                                               password=self.__db_password,
	                                       host=self.__db_host,
			                       port=self.__db_port
			                       )
      self.cur = None
    except psycopg2.Error:
      raise Exception("Did you define database parameters in config and run make?")
    
  def __del__(self):
    self.conns.closeall()
      