import cherrypy
import config
import json
import pandas
from db.connect import Connection
class Donations():
  '''Define Root webpages'''
  def __init__(self, Connection, api_key):
    from analysis.search import SearchLocation
    try:
      from bs4 import BeautifulSoup as bs
    except ImportError:
      from BeautifulSoup import BeautifulSoup as bs
    self.__SearchLocation = SearchLocation
    self.__bs = bs
    self.__Connection = Connection
    self.__api_key = api_key
    self.searches = {}
    
  def index(self):
    return "Hello, this is a subdirectory"