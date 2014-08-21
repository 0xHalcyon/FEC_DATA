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
      body {{ height: 100%; margin: 0; padding: 0; }}
      .overlay {{ position: absolute; top: 0; left: 0; width: 100%; height: 100%; z-index: 2; background-color: rgba(0,0,0,0.5);}}
      #map-canvas {{ height: 100%; margin: 0; position: relative; z-index:1}}
    </style>
    <script type="text/javascript"
      src="https://maps.googleapis.com/maps/api/js?key={0}">
    </script>
    <script type="text/javascript">
      function initialize() {{
        var positionMap = new google.maps.LatLng(39.50, -98.35);
        
        var mapOptions = {{
          center: positionMap,
          disableDefaultUI: true,
          zoom: 4
        }};
	
        var map = new google.maps.Map(document.getElementById("map-canvas"),
            mapOptions);
            
	var searchBoxString = '<div id="search-bar">' +
                               '<form class="form-wrapper">' +
                               '<input type="text" id="search" placeholder="Search by Name, City, State, or Zipcode ..." required>' +
                               '<input type="submit" value="go" id="submit">' +
                               '</form>' +
                               '</div>';
                               
        var searchbox = new google.maps.InfoWindow({{content: searchBoxString}});
        
        var marker = new google.maps.Marker({{
					      position: positionMap,
					      map: map,
					      title: 'FEC WATCHDOGS'
					    }};
	google.maps.event.addListener(marker, 'load', function() { searchbox.open(map, marker);});
      }}
      google.maps.event.addDomListener(window, 'load', initialize);
    </script>
  </head>
  <body>
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
    