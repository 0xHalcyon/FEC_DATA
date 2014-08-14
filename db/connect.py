#!/usr/bin/env python
import psycopg2

class Connection:
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
      