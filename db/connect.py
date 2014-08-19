#!/usr/bin/env python
import psycopg2

class Connection:
  def __init__(self, conn_settings):
    self.__geodb = "geozipcodes"
    self.__db_name = conn_settings['db_prefix']
    self.__db_user = conn_settings['db_user']
    self.__db_password = conn_settings['db_password']
    self.__db_host = conn_settings['db_host']
    self.__db_port = conn_settings['db_port']
    try:       
      self.conn = psycopg2.connect(database=self.__db_name.lower(), \
                                       user=self.__db_user,
                                       password=self.__db_password,
	                               host=self.__db_host,
			               port=self.__db_port
			               )
      self.conn.set_client_encoding("UTF8")
      self.cur = self.fec_conn.cursor()
    except psycopg2.Error:
      raise Exception("Did you define database parameters in config and run make?")
    
  def __del__(self):
    self.cur.close()
    self.conn.close()
      