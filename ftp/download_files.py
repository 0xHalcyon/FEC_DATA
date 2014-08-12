#!/usr/bin/python

from ftplib import FTP
import os

try:
  import config
except ImportError:
  os.symlink("config.py", "db/config.py")
  import config
  
url = "ftp://ftp.fec.gov"
ftp =FTP("ftp.fec.gov")

if not os.path.isdir("data"):
  os.mkdir("data")
  

ftp.login()

files = {"/FEC/%s/cm%s.zip"    : "data/%s/cm%s.zip",   # Committee Master File
         "/FEC/%s/cn%s.zip"    : "data/%s/cn%s.zip",   # Candidate Master File
	 "/FEC/%s/ccl%s.zip"   : "data/%s/ccl%s.zip",   # Candidate Committee Linkage File
	 "/FEC/%s/oth%s.zip"   : "data/%s/oth%s.zip",  # Any transaction from one committee to another
	 "/FEC/%s/pas2%s.zip"  : "data/%s/pas2%s.zip", # Contributions to candidates (and other expenditures) from committees
	 "/FEC/%s/indiv%s.zip" : "data/%s/indiv%s.zip" # Contributions by individuals
	}

files_1998 = {"/FEC/%s/cm%s.zip"    : "data/%s/cm%s.zip",   # Committee Master File
              "/FEC/%s/cn%s.zip"    : "data/%s/cn%s.zip",   # Candidate Master File
	      "/FEC/%s/oth%s.zip"   : "data/%s/oth%s.zip",  # Any transaction from one committee to another
	      "/FEC/%s/pas2%s.zip"  : "data/%s/pas2%s.zip", # Contributions to candidates (and other expenditures) from committees
	      "/FEC/%s/indiv%s.zip" : "data/%s/indiv%s.zip" # Contributions by individuals
             }

for year in range(config.start_year, config.end_year, 2):
  year_suffix = str(year)[2:]
  
  if year <= 1998:
    
    for f in sorted(files_1998):
      if not os.path.isdir("data/%s" % year):
        os.mkdir("data/%s"  % year)
      to_get = f % (year, year_suffix)
      save_to = files_1998[f] % (year, year_suffix)
      print "Downloading: %s%s" % (url, to_get)
      ftp.retrbinary("RETR %s" % to_get, open(save_to, "wb").write)
      print "Saving to: %s" % save_to
      print "Done"
      
  else:    
    
    for f in sorted(files):
      if not os.path.isdir("data/%s" % year):
        os.mkdir("data/%s"  % year)
      to_get = f % (year, year_suffix)
      save_to = files[f] % (year, year_suffix)
      print "Downloading: %s%s" % (url, to_get)
      ftp.retrbinary("RETR %s" % to_get, open(save_to, "wb").write)
      print "Saving to: %s" % save_to
      print "Done"