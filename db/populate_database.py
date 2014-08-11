#!/usr/bin/env python

import psycopg2
from datetime import datetime
# files list
#(year, year_suffix)

def read_some_lines(file_object, chunk_size=1024):
  while True:
    data = file_object.readlines(chunk_size)
    if not data:
      break
    yield data
    
files = {"committee_master" : ["../data/%s/cm%s/cm.txt", "cm_header_file.csv"],
             "candidate_master" : ["../data/%s/cn%s/cn.txt", "cn_header_file.csv"],
             "candidate_linkage" : ["../data/%s/ccl%s/ccl.txt", "ccl_header_file.csv"],
             "comm_to_comm" : ["../data/%s/oth%s/itoth.txt","oth_header_file.csv"],
             "cand_to_comm" : ["../data/%s/pas2%s/itpas2.txt", "pas2_header_file.csv"],
             "indiv_contrib" : ["../data/%s/indiv%s/itcont.txt", "indiv_header_file.csv"]}
i = 0
for year in range(2004, 2015, 2):
  year_suffix = str(year)[2:]
  conn = psycopg2.connect(dbname="FEC_%s" %year, user="postgres")
  cur = conn.cursor()
  for f in files:
    print "CURRENT YEAR: %s\nCURRENT TABLE%s" % (year, f)
    temp = open(files[f][0] % (year, year_suffix))
    template = open(files[f][1]).read().strip()
    template = str(tuple(template.split(","))).replace("'", "").lower()
    for chunk in read_some_lines(temp):
      for line in chunk:
	temp1 = line.strip().replace("'", "").split("|")
	if f in ("comm_to_comm", "cand_to_comm", "indiv_contrib"):
	  if temp1[13]:
	    date = temp1[13]
	    date = datetime(month=int(date[0:2]), day=int(date[2:4]), year=int(date[4:]))
	    temp1[13] = date.strftime("%Y%m%d")
	  else :
	    date = datetime(month=01, day=01, year=1900)
	    temp1[13] = date.strftime("%Y%m%d")	   
	temp1 = tuple(temp1)
	query = "INSERT INTO %s %s VALUES %s;" % (f, template, temp1)
        #print query
        cur.execute(query)
        i+=1
      conn.commit()
    print "Total INSERTs so far %i" % i

    
