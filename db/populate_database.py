#!/usr/bin/env python

import psycopg2
import os
from datetime import datetime

try:
  import config
except ImportError:
  os.symlink("../config.py", "./config.py")
  import config
  

# files list
#(year, year_suffix)

def read_some_lines(file_object, chunk_size=1024):
  while True:
    data = file_object.readlines(chunk_size)
    if not data:
      break
    yield data
    
files = {"committee_master"  : ["data/%s/cm%s/cm.txt", "db/headers/cm_header_file.csv"],
         "candidate_master"  : ["data/%s/cn%s/cn.txt", "db/headers/cn_header_file.csv"],
         "candidate_linkage" : ["data/%s/ccl%s/ccl.txt", "db/headers/ccl_header_file.csv"],
         "comm_to_comm"      : ["data/%s/oth%s/itoth.txt","db/headers/oth_header_file.csv"],
         "cand_to_comm"      : ["data/%s/pas2%s/itpas2.txt", "db/headers/pas2_header_file.csv"],
         "indiv_contrib"     : ["data/%s/indiv%s/itcont.txt", "db/headers/indiv_header_file.csv"]}

files_1998 = {"committee_master"  : ["data/%s/cm%s/cm.txt", "db/headers/cm_header_file.csv"],
              "candidate_master"  : ["data/%s/cn%s/cn.txt", "db/headers/cn_header_file.csv"],
              "comm_to_comm"      : ["data/%s/oth%s/itoth.txt","db/headers/oth_header_file.csv"],
              "cand_to_comm"      : ["data/%s/pas2%s/itpas2.txt", "db/headers/pas2_header_file.csv"],
              "indiv_contrib"     : ["data/%s/indiv%s/itcont.txt", "db/headers/indiv_header_file.csv"]}

if not os.path.isdir("db/errors"):
  os.mkdir("db/errors")
  
errors = open("db/errors/errors.txt", "wb")
for year in range(config.start_year, config.end_year, 2):
  year_suffix = str(year)[2:]
  conn = psycopg2.connect(dbname=config.db_prefix.lower()+str(year),
                          user=config.db_user,
                          password=config.db_password,
			  host=config.db_host,
			  port=config.db_port
			  )
  cur = conn.cursor()
  if year <= 1998:
    
    for f in sorted(files_1998):
      print "CURRENT YEAR: %s\nCURRENT TABLE: %s" % (year, f)
      temp = open(files[f][0] % (year, year_suffix))
      template = open(files[f][1]).read().strip()
      template = str(tuple(template.split(","))).replace("'", "").lower()
      for chunk in read_some_lines(temp):
        for line in chunk:
	  temp1 = line.strip().replace("'", "").split("|")
	  if f in ("comm_to_comm", "cand_to_comm", "indiv_contrib"):
	    if temp1[13]:
	      date = temp1[13]
	      try:
	        date = datetime(month=int(date[0:2]), day=int(date[2:4]), year=int(date[4:]))
	      except ValueError as e:
		print e, temp1[13]
		to_write = "%s|%s\n" % (year, str(temp1))
		errors.write(to_write)
		errors.flush()
		continue
	      temp1[13] = date.strftime("%Y%m%d")
	    else:
	      date = datetime(month=01, day=01, year=1900)
	      temp1[13] = date.strftime("%Y%m%d")	   
	  temp1 = tuple(temp1)
	  query = "INSERT INTO %s %s VALUES %s;" % (f, template, temp1)
          #print query
          cur.execute(query)
          #i+=1
      conn.commit()
      
  else:
    
    for f in sorted(files):
      print "CURRENT YEAR: %s\nCURRENT TABLE: %s" % (year, f)
      temp = open(files[f][0] % (year, year_suffix))
      template = open(files[f][1]).read().strip()
      template = str(tuple(template.split(","))).replace("'", "").lower()
      for chunk in read_some_lines(temp):
        for line in chunk:
          temp1 = line.strip().replace("'", "").split("|")
 	  if f in ("comm_to_comm", "cand_to_comm", "indiv_contrib"):
	    if temp1[13]:
	      date = temp1[13]
	      try:
	        date = datetime(month=int(date[0:2]), day=int(date[2:4]), year=int(date[4:]))
	      except ValueError as e:
		print e, temp1[13]
		errors.write("%s|%s\n" % (year, str(temp1)))
		errors.flush()
		continue
	      temp1[13] = date.strftime("%Y%m%d")
	    else :
	      date = datetime(month=01, day=01, year=1900)
	      temp1[13] = date.strftime("%Y%m%d")	   
	  temp1 = tuple(temp1)
	  query = "INSERT INTO %s %s VALUES %s;" % (f, template, temp1)
          #print query
          cur.execute(query)
          #i+=1
        conn.commit()
      #print "Total INSERTs so far %i" % i

    
