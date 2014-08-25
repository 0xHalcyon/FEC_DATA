#!/usr/bin/env python

import psycopg2
from psycopg2 import pool
import os
from datetime import datetime
import threading
def read_some_lines(file_object, chunk_size=1024):
  while True:
    data = file_object.readlines(chunk_size)
    if not data:
      break
    yield data   
    
class threadedPopulate(threading.Thread):
  def __init__(self, threadID, year, cwd, Connections):
    threading.Thread.__init__(self)
    self.cwd = cwd
    self.threadID = threadID
    self.name = year
    self.conn = Connections.conns.getconn(key=self.name)
    self.complete = False
  def run(self):
    print "Starting to populate %s" % self.name
    populate_db = PopulateDatabase(self.cwd, self.name, self.conn)
    populate_db.populate_database()
    self.complete = True
    print "Done populating %s" % self.name    

class PopulateDatabase():
  def __init__(self, cwd, year, Connection):
    self.cwd = cwd
    self.year = year
    self.__Connection = Connection
    self.__cur = self.__Connection.cursor()
    self.files = {"committee_master_%s"  : ["%s/data/%s/cm%s/cm.txt", "%s/db/headers/cm_header_file.csv"],
                  "candidate_master_%s"  : ["%s/data/%s/cn%s/cn.txt", "%s/db/headers/cn_header_file.csv"],
                  "candidate_linkage_%s" : ["%s/data/%s/ccl%s/ccl.txt", "%s/db/headers/ccl_header_file.csv"],
                  "comm_to_comm_%s"      : ["%s/data/%s/oth%s/itoth.txt","%s/db/headers/oth_header_file.csv"],
                  "cand_to_comm_%s"      : ["%s/data/%s/pas2%s/itpas2.txt", "%s/db/headers/pas2_header_file.csv"],
                  "indiv_contrib_%s"     : ["%s/data/%s/indiv%s/itcont.txt", "%s/db/headers/indiv_header_file.csv"]
                 }

    self.files_1998 = {"committee_master_%s"  : ["%s/data/%s/cm%s/cm.txt", "%s/db/headers/cm_header_file.csv"],
                       "candidate_master_%s"  : ["%s/data/%s/cn%s/cn.txt", "%s/db/headers/cn_header_file.csv"],
                       "comm_to_comm_%s"      : ["%s/data/%s/oth%s/itoth.txt","%s/db/headers/oth_header_file.csv"],
                       "cand_to_comm_%s"      : ["%s/data/%s/pas2%s/itpas2.txt", "%s/db/headers/pas2_header_file.csv"],
                       "indiv_contrib_%s"     : ["%s/data/%s/indiv%s/itcont.txt", "%s/db/headers/indiv_header_file.csv"]
                      }
    if not os.path.isdir("%s/db/errors" % self.cwd):
      os.mkdir("%s/db/errors" % self.cwd)
  
    self.errors = open("%s/db/errors/errors_%s.txt" % (self.cwd, self.year), "wb")
    
  def log_error(self, table, string):
    to_write = "%s::%s\n" % (table, string)
    self.errors.write(to_write)
    self.errors.flush()
    
  def populate_database(self):
    """Populates FEC databases based on parameters listed in the config.py file in the root of this package"""
    
    print "CURRENT YEAR: %s" % self.year
    year_suffix = str(self.year)[2:]
    
    if self.year <= 1998:
      temp_files = self.files_1998
    else:
      temp_files = self.files
      
    for table in sorted(temp_files):
      f = table % self.year
      print "CURRENT TABLE: %s" % f
      try:
        temp = open(temp_files[table][0] % (self.cwd, self.year, year_suffix))
        template = open(temp_files[table][1] % self.cwd).read().strip()
      except IOError:
        print "Have you run 'make download && make extract yet'?"
	os.sys.exit(1)
      template = str(tuple(template.split(","))).replace("'", "").lower()
      for chunk in read_some_lines(temp):
        for line in chunk:
	  temp1 = line.replace("\x92", "")
	  temp1 = temp1.replace("\xa0", " ")
	  temp1 = temp1.replace("\x85", "...")
	  temp1 = temp1.strip().replace("'", "").split("|")
	  
	  if f in ("comm_to_comm_%s" % self.year, "cand_to_comm_%s" % self.year, "indiv_contrib_%s" % self.year):
	    if temp1[13]:
	      date = temp1[13]
	      try:
	        date = datetime(month=int(date[0:2]), day=int(date[2:4]), year=int(date[4:]))
	      except ValueError as e:
	        temp1[13] = datetime(month=1, day=1, year=self.year).strftime("%Y%m%d")
		self.log_error(f, line)
		continue
	      temp1[13] = date.strftime("%Y%m%d")
	    else:
	      date = datetime(month=01, day=01, year=self.year)
	      self.log_error(f, line)
	      temp1[13] = date.strftime("%Y%m%d")	   
	       
	  elif f == "committee_master_%s" % self.year:
	    cmte_zip = temp1[7]
	    try:
	      cmte_zip = int(cmte_zip[0:5])
	      temp1[7] = cmte_zip
	    except ValueError as e:
              temp1[7] = 0

	  elif f == "candidate_master_%s" % self.year:
	    cand_zip = temp1[14]
	    try:
              cand_zip = int(cand_zip[0:5])
	      temp1[14] = cand_zip
	    except ValueError as e:
	      temp1[14] = 0
		
	  temp1 = tuple(temp1)
	  try:
	    query = "INSERT INTO %s %s VALUES %s;" % (f, template, temp1)
	    self.__cur.execute("BEGIN;")
	    self.__cur.execute("SAVEPOINT save_point;")
            self.__Connection.cur.execute(query)
          except (psycopg2.DataError, psycopg2.InternalError) as e:
            self.log_error(f, e + "::" + line)
	    self.__cur.execute("ROLLBACK TO SAVEPOINT save_point;")
	    continue
	  except psycopg2.IntegrityError as e:
            self.log_error(f, e + "::" + line)
	    self.__cur.execute("ROLLBACK TO SAVEPOINT save_point;")
	    continue
	  else:
	    self.__cur.execute("RELEASE SAVEPOINT save_point;")
      self.__Connection.commit()
    self.__cur.close()
    self.errors.close()
