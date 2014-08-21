#!/usr/bin/env python
import cherrypy
import json
import config
from db.connect import Connection
from analysis.search import SearchLocation

class Root():
  '''Define Root webpages'''
  def __init__(self, Connection, SearchLocation, api_key):
    self.__Connection = Connection
    self.__SearchLocation = SearchLocation
    self.__api_key = api_key
  @cherrypy.expose
  def index(self):
    page = """<!DOCTYPE html>
<html>
  <head>
    <meta name="viewport" content="initial-scale=1.0, user-scalable=no" />
    <link rel="stylesheet" href="/main/form.css">
    <style type="text/css">
      html {{ height: 100% }}
      body {{ height: 100%; margin: 0; padding: 0 }}
      #map-canvas {{ height: 90%; margin-top: 10%;}}
      #search-bar {{ height: 10%; margin: 0; padding: 0}}
    </style>
    <script type="text/javascript"
      src="https://maps.googleapis.com/maps/api/js?key={0}">
    </script>
    <script type="text/javascript">
      function initialize() {{
        var mapOptions = {{
          center: new google.maps.LatLng(39.50, -98.35),
          zoom: 6
        }};
        var map = new google.maps.Map(document.getElementById("map-canvas"),
            mapOptions);
      }}
      google.maps.event.addDomListener(window, 'load', initialize);
    </script>
  </head>
  <body>
    <div id="search-bar">
      <form class="form-wrapper">
        <input type="text" id="search" placeholder="Search for CSS3, HTML5, jQuery ..." required>
        <input type="submit" value="go" id="submit">
      </form>
    <div id="map-canvas"/>
  </body>
</html>

""".format(self.__api_key)
    return page
  def search(self, params):
    pass

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
    