#!/usr/bin/env python
import cherrypy
import json
import config
from db.connect import Connection
from analysis.search import SearchLocation

class Root():
  '''Define Root webpages'''
  def __init__(self, Connection, SearchLocation):
    self.__Connection = Connection
    self.__SearchLocation = SearchLocation
    
  @cherrypy.expose
  def index(self):
    page = """<!DOCTYPE html>
<html>
<head>
</head>
<body>
<iframe width="100%" height="100%" frameborder="0" style="border:0" src="https://www.google.com/maps/embed/v1/place?q=United%20States&key=AIzaSyDWgL3zs2CJN970M8VvP4gseHkym1Tc-qs"></iframe>
</body>
</html>
"""
    return page
  if __name__ == '__main__':
    conn_settings = {'db_password': config.db_password, 
                 'db_user': config.db_user,
                 'db_host': config.db_host,
                 'db_port': config.db_port,
                 'db_name': config.db_name,
                 'start_year': config.start_year,
                 'end_year': config.end_year
                }
    c = Connection(conn_settings)
    s = SearchLocation(c)
    cherrypy.quickstart(Root(c, s), '/')
    