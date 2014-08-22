#!/usr/bin/env python
import cherrypy
import json
import config
import json
import pandas
from cStringIO import StringIO
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
    <link rel="stylesheet" href="/css/form.css">
    <style type="text/css">
      html {{ height: 100% }}
      body {{ height: 100%; margin: 0; padding: 0; }}
      #map-canvas {{ height: 100%; margin: 0;}}
    </style>
    <script type="text/javascript"
      src="https://maps.googleapis.com/maps/api/js?key={0}">
    </script>
    <script type="text/javascript">
      function initialize() {{
        var positionMap = new google.maps.LatLng(39.50, -98.35);
        var positionMarker = new google.maps.LatLng(32, -98.35);
        var mapOptions = {{
          center: positionMap,
          disableDefaultUI: true,
          zoom: 5
        }};
	
        var map = new google.maps.Map(document.getElementById("map-canvas"),
            mapOptions);
            
	var searchBoxString = '<div id="search-bar">' +
                               '<form class="form-wrapper" action="searchByName" method="post">' +
                               '<input type="text" name="searchByName" id="searchByName" placeholder="Search by Name..." required>' +
                               '<input type="submit" value="Go!" id="submitName">' +
                               '</form>' +
                               '<form class="form-wrapper" action="searchByCityState" method="post">' +
                               '<input type="text" name="searchByCity" id="searchByCity" placeholder="...City/State (Specify only state to search entire state)...">' +
                               '<select name="searchByState" id="searchByState">' +
                               '       <option value="AL">Alabama</option>' +
                               '       <option value="AK">Alaska</option>' +
                               '       <option value="AZ">Arizona</option>' +
                               '       <option value="AR">Arkansas</option>' +
                               '       <option value="CA">California</option>' +
                               '       <option value="CO">Colorado</option>' +
                               '       <option value="CT">Connecticut</option>' +
                               '       <option value="DE">Delaware</option>' +
                               '       <option value="DC">District Of Columbia</option>' +
                               '       <option value="FL">Florida</option>' +
                               '       <option value="GA">Georgia</option>' +
                               '       <option value="HI">Hawaii</option>' +
                               '       <option value="ID">Idaho</option>' +
                               '       <option value="IL">Illinois</option>' +
                               '       <option value="IN">Indiana</option>' +
                               '       <option value="IA">Iowa</option>' +
                               '       <option value="KS">Kansas</option>' +
                               '       <option value="KY">Kentucky</option>' +
                               '       <option value="LA">Louisiana</option>' +
                               '       <option value="ME">Maine</option>' +
                               '       <option value="MD">Maryland</option>' +
                               '       <option value="MA">Massachusetts</option>' +
                               '       <option value="MI">Michigan</option>' +
                               '       <option value="MN">Minnesota</option>' +
                               '       <option value="MS">Mississippi</option>' +
                               '       <option value="MO">Missouri</option>' +
                               '       <option value="MT">Montana</option>' +
                               '       <option value="NE">Nebraska</option>' +
                               '       <option value="NV">Nevada</option>' +
                               '       <option value="NH">New Hampshire</option>' +
                               '       <option value="NJ">New Jersey</option>' +
                               '       <option value="NM">New Mexico</option>' +
                               '       <option value="NY">New York</option>' +
                               '       <option value="NC">North Carolina</option>' +
                               '       <option value="ND">North Dakota</option>' +
                               '       <option value="OH">Ohio</option>' +
                               '       <option value="OK">Oklahoma</option>' +
                               '       <option value="OR">Oregon</option>' +
                               '       <option value="PA">Pennsylvania</option>' +
                               '       <option value="RI">Rhode Island</option>' +
                               '       <option value="SC">South Carolina</option>' +
                               '       <option value="SD">South Dakota</option>' +
                               '       <option value="TN">Tennessee</option>' +
                               '       <option value="TX">Texas</option>' +
                               '       <option value="UT">Utah</option>' +
                               '       <option value="VT">Vermont</option>' +
                               '       <option value="VA">Virginia</option>' +
                               '       <option value="WA">Washington</option>' +
                               '       <option value="WV">West Virginia</option>' +
                               '       <option value="WI">Wisconsin</option>' +
                               '       <option value="WY">Wyoming</option>' +
                               '</select>      ' +
                               '<input type="submit" value="Go!" id="submitCity">' +
                               '</form>' +
                               '<form class="form-wrapper" action="searchByZipcode" method="post">' +
                               '<input type="text" name="searchByZip" id="searchByZip" placeholder="...Or Zipcode..." required>' +
                               '<select name="distanceRadius" id="searchDistance">' +
                               '<option value="50">50</option>' +
                               '<option value="25">25</option>' +
                               '<option value="10">10</option>' +
                               '<option value="5">5</option>' +
                               '</select>' +
                               '<select name="distanceUnit" id="searchUnit">' +
                               '<option value="miles">Miles</option>' +
                               '<option value="kilometers">Kilometers</option>' +
                               '</select>' +
                               '<input type="submit" value="Go!" id="submitZipcode">'+
                               '</form>' +
                               '</div>';
                               
        var searchbox = new google.maps.InfoWindow({{content: searchBoxString}});
        
        var marker = new google.maps.Marker({{
					      position: positionMarker,
					      map: map,
					      title: 'FEC WATCHDOGS',
					      icon: '/images/logo.png'
					    }});
	
	google.maps.event.addListener(marker, 'click', function() {{ searchbox.open(map, marker);}});
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
  
  @cherrypy.expose
  def searchByZipcode(self, searchByZip="", distanceRadius=50, distanceUnit="kilometers"):
    if not searchByZip:
      return "Please enter a valid Zipcode"
    try:
      zipcode = int(searchByZip)
    except TypeError:
      return "Please enter a valid Zipcode"
    try:
      distance = int(distanceRadius)
    except TypeError:
      return "Please enter a valid search radius"
    parameters = {'zipcode':zipcode, 'distance':distance, 'unit':distanceUnit}
    cherrypy.response.headers['Content-Type'] = 'text/html'
    cand_ids, cand_comms = self.__SearchLocation.search_names_by_zip(parameters)
    cand_ids = pandas.read_json(json.dumps(cand_ids))
    cand_comms = pandas.read_json(json.dumps(cand_comms), orient='index')
    return cand_ids.to_html()+cand_comms.to_html()
  
  @cherrypy.expose
  def searchByCityState(self, searchByCity="", searchByState=""):
    if not searchByState:
      return "Please enter a valid city/state"
    cand_st = searchByState
    cand_city = searchByCity
    parameters = {'cand_st': cand_st, 'cand_city': cand_city}
    cherrypy.response.headers['Content-Type'] = 'text/html'
    cand_ids, cand_comms = self.__SearchLocation.search_by_city_state(parameters)
    cand_ids = pandas.read_json(json.dumps(cand_ids))
    cand_comms = pandas.read_json(json.dumps(cand_comms), orient='index')
    return cand_ids.to_html()+cand_comms.to_html()  
  
  @cherrypy.expose
  def searchByName(self, searchByName=""):
    if not searchByName:
      return "Please enter a valid name"
    parameters = {'name':searchByName}
    cherrypy.response.headers['Content-Type'] = 'text/html'
    cand_ids, cand_comms = self.__SearchLocation.search_by_name(parameters)
    cand_ids = pandas.read_json(json.dumps(cand_ids))
    cand_comms = pandas.read_json(json.dumps(cand_comms), orient='index')
    return cand_ids.to_html()+cand_comms.to_html()
  
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
    