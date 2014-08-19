#!/usr/bin/env python
import psycopg2
import os
import config
from connect import Connection
from datetime import datetime
from ftp.download_files import download_files
from ftp.extract_files import extract_files

class UpdateDB():
  '''Updates database entries from last update check'''
  def __init__(self):
    self.__Connection = Connection
    self.files = {
                    "committee_master"  : ["%s/data/%s/cm%s/cm.txt", "%s/db/headers/cm_header_file.csv"],
                    "candidate_master"  : ["%s/data/%s/cn%s/cn.txt", "%s/db/headers/cn_header_file.csv"],
                    "candidate_linkage" : ["%s/data/%s/ccl%s/ccl.txt", "%s/db/headers/ccl_header_file.csv"],
                    "comm_to_comm"      : ["%s/data/%s/oth%s/itoth.txt","%s/db/headers/oth_header_file.csv"],
                    "cand_to_comm"      : ["%s/data/%s/pas2%s/itpas2.txt", "%s/db/headers/pas2_header_file.csv"],
                    "indiv_contrib"     : ["%s/data/%s/indiv%s/itcont.txt", "%s/db/headers/indiv_header_file.csv"]
                 }

    self.files_1998 = {
                         "committee_master"  : ["%s/data/%s/cm%s/cm.txt", "%s/db/headers/cm_header_file.csv"],
                         "candidate_master"  : ["%s/data/%s/cn%s/cn.txt", "%s/db/headers/cn_header_file.csv"],
                         "comm_to_comm"      : ["%s/data/%s/oth%s/itoth.txt","%s/db/headers/oth_header_file.csv"],
                         "cand_to_comm"      : ["%s/data/%s/pas2%s/itpas2.txt", "%s/db/headers/pas2_header_file.csv"],
                         "indiv_contrib"     : ["%s/data/%s/indiv%s/itcont.txt", "%s/db/headers/indiv_header_file.csv"]
                      }
    
    self.update_stmt = "UPDATE %s SET %s='%s' WHERE %s='%s';"
    self.insert_stmt = "INSERT INTO %s %s VALUES %s;"
    self.select_stmt = "SELECT %s FROM %s WHERE %s='%s';"
    self.download_files = download_files
    self.extract_files = extract_files
    self.cwd = config.cwd
    self.start_year = config.start_year
    self.end_year = config.end_year
    self.__conn_settings = {'db_password': config.db_password, 
			    'db_user': config.db_user,
			    'db_host': config.db_host,
			    'db_port': config.db_port,
			    'db_prefix': config.db_prefix,
			    'year': 0
			    }
  def read_some_lines(self, file_object, chunk_size=1024):
    while True:
      data = file_object.readlines(chunk_size)
      if not data:
        break
      yield data
      
  def format_date(self, datestr)
    if datestr:
      date = datestr
      try:
	date = datetime(month=int(date[0:2]), day=int(date[2:4]), year=int(date[4:]))
      except ValueError as e:
	print "Error: %s %s\nContinuing" % (e, datestr)
	to_write = "%s|%s\n" % (year, str(temp1))
	errors.write(to_write)
	errors.flush()
	continue
      date = date.strftime("%Y%m%d")
    else:
      date = datetime(month=01, day=01, year=1900)
      date = date.strftime("%Y%m%d")
    return date
  
  def update(self):
    self.download_files(self.start_year, self.end_year, self.cwd)
    self.extract_files(self.start_year, self.end_year, self.cwd)
    
    for year in range(self.start_year, self.end_year, 2):
      temp_files = self.files
      if year <= 1998:
	temp_files = self.files_1998
      self.__conn_settings['year'] = year
      __conn = self.__Connection(self.__conn_settings)
      for f in sorted(temp_files):
        print "CURRENT YEAR: %s\nCURRENT TABLE: %s" % (year, f)
        try:
          temp = open(files[f][0] % (cwd, year, year_suffix))
          template = open(files[f][1] % cwd).read().strip()
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
	    
            if f in ("comm_to_comm", "cand_to_comm", "indiv_contrib"):
	      query = self.select_stmt % ('sub_id', temp1[21])
	      __conn.fec_cur.execute(query)
	      results = __conn.fec_cur.fetchall()
	      
	      if len(results) == 0:
		print "New entry %s" % temp1[21]
		temp1[13] = format_date(temp1[13])
		temp1 = tuple(temp1)
		
	        try:
	          query = "INSERT INTO %s %s VALUES %s;" % (f, template, temp1)
                  cur.execute(query)
                except (psycopg2.DataError, psycopg2.InternalError) as e:
                  print "Error: %s %s\nContinuing" % (e, temp1[13])
                  to_write = "%s|%s\n" % (year, str(temp1))
	          errors.write(to_write)
	          errors.flush()
	          conn.rollback()
	         continue
	        except psycopg2.IntegrityError as e:
	          print "Database already populated! Exiting NOW!"
	          os.sys.exit(1)

      
    