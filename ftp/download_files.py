#!/usr/bin/python

from ftplib import FTP
import os
  
def download_files(start_year, end_year, cwd):
  url = "ftp://ftp.fec.gov"
  ftp =FTP("ftp.fec.gov")
  ftp.login()

  if not os.path.isdir("data"):
    os.mkdir("data")
  
  files = {"/FEC/%s/cm%s.zip"    : "%s/data/%s/cm%s.zip",   # Committee Master File
           "/FEC/%s/cn%s.zip"    : "%s/data/%s/cn%s.zip",   # Candidate Master File
	   "/FEC/%s/ccl%s.zip"   : "%s/data/%s/ccl%s.zip",   # Candidate Committee Linkage File
	   "/FEC/%s/oth%s.zip"   : "%s/data/%s/oth%s.zip",  # Any transaction from one committee to another
	   "/FEC/%s/pas2%s.zip"  : "%s/data/%s/pas2%s.zip", # Contributions to candidates (and other expenditures) from committees
	   "/FEC/%s/indiv%s.zip" : "%s/data/%s/indiv%s.zip" # Contributions by individuals
	  }

  files_1998 = {"/FEC/%s/cm%s.zip"    : "%s/data/%s/cm%s.zip",   # Committee Master File
                "/FEC/%s/cn%s.zip"    : "%s/data/%s/cn%s.zip",   # Candidate Master File
	        "/FEC/%s/oth%s.zip"   : "%s/data/%s/oth%s.zip",  # Any transaction from one committee to another
	        "/FEC/%s/pas2%s.zip"  : "%s/data/%s/pas2%s.zip", # Contributions to candidates (and other expenditures) from committees
	        "/FEC/%s/indiv%s.zip" : "%s/data/%s/indiv%s.zip" # Contributions by individuals
               }

  for year in range(start_year, end_year, 2):
    year_suffix = str(year)[2:]
  
    if year <= 1998:
     
      for f in sorted(files_1998):
        if not os.path.isdir("%s/data/%s" % (cwd, year)):
          os.mkdir("%s/data/%s"  % (cwd, year))
        to_get = f % (year, year_suffix)
        save_to = files_1998[f] % (cwd, year, year_suffix)
        print "Downloading: %s%s" % (url, to_get)
        ftp.retrbinary("RETR %s" % to_get, open(save_to, "wb").write)
        print "Saving to: %s" % save_to
        print "Done"
      
    else:    
    
      for f in sorted(files):
        if not os.path.isdir("%s/data/%s" % (cwd, year)):
          os.mkdir("%s/data/%s"  % (cwd, year))
        to_get = f % (year, year_suffix)
        save_to = files[f] % (cwd, year, year_suffix)
        print "Downloading: %s%s" % (url, to_get)
        ftp.retrbinary("RETR %s" % to_get, open(save_to, "wb").write)
        print "Saving to: %s" % save_to
        print "Done"