#!/usr/bin/env python
import zipfile
import os

try:
  import config
except ImportError:
  os.symlink("./config.py", "../config.py")
  import config

files = {"data/%s/cm%s.zip"    : "data/%s/cm%s/",    # Committee Master File
         "data/%s/cn%s.zip"    : "data/%s/cn%s/",    # Candidate Master File
	 "data/%s/ccl%s.zip"   : "data/%s/ccl%s/",    # Candidate Committee Linkage File
	 "data/%s/oth%s.zip"   : "data/%s/oth%s/",   # Any transaction from one committee to another
	 "data/%s/pas2%s.zip"  : "data/%s/pas2%s/",  # Contributions to candidates (and other expenditures) from committees
	 "data/%s/indiv%s.zip" : "data/%s/indiv%s/", # Contributions by individuals
	}

files_1998 = {"data/%s/cm%s.zip"    : "data/%s/cm%s/",    # Committee Master File
              "data/%s/cn%s.zip"    : "data/%s/cn%s/",    # Candidate Master File
	      "data/%s/oth%s.zip"   : "data/%s/oth%s/",   # Any transaction from one committee to another
	      "data/%s/pas2%s.zip"  : "data/%s/pas2%s/",  # Contributions to candidates (and other expenditures) from committees
	      "data/%s/indiv%s.zip" : "data/%s/indiv%s/", # Contributions by individuals
	     }

if not os.path.isdir("data"):
  os.mkdir("data")
  
for year in range(config.start_year, config.end_year, 2):
  year_suffix = str(year)[2:]
  
  if year <= 1998:
    
    for f in files_1998:
      archive = f % (year, year_suffix)
      extract_to = files_1998[f] % (year, year_suffix)
      if not os.path.isdir(extract_to):
        os.mkdir(extract_to)
      zf = zipfile.ZipFile(archive)
      print "Extracting %s to %s" % (archive, extract_to)
      zf.extractall(extract_to)
      
  else: 
    
    for f in files:
      archive = f % (year, year_suffix)
      extract_to = files[f] % (year, year_suffix)
      if not os.path.isdir(extract_to):
        os.mkdir(extract_to)
      zf = zipfile.ZipFile(archive)
      print "Extracting %s to %s" % (archive, extract_to)
      zf.extractall(extract_to)
    