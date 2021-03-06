#!/usr/bin/env python
import zipfile
import os

def extract_files(start_year, end_year, cwd):
  """Extracts files downloaded from the FEC's FTP server"""
  files = {"%s/data/%s/cm%s.zip"    : "%s/data/%s/cm%s/",    # Committee Master File
           "%s/data/%s/cn%s.zip"    : "%s/data/%s/cn%s/",    # Candidate Master File
	   "%s/data/%s/ccl%s.zip"   : "%s/data/%s/ccl%s/",    # Candidate Committee Linkage File
	   "%s/data/%s/oth%s.zip"   : "%s/data/%s/oth%s/",   # Any transaction from one committee to another
	   "%s/data/%s/pas2%s.zip"  : "%s/data/%s/pas2%s/",  # Contributions to candidates (and other expenditures) from committees
	   "%s/data/%s/indiv%s.zip" : "%s/data/%s/indiv%s/", # Contributions by individuals
	  }

  files_1998 = {"%s/data/%s/cm%s.zip"    : "%s/data/%s/cm%s/",    # Committee Master File
                "%s/data/%s/cn%s.zip"    : "%s/data/%s/cn%s/",    # Candidate Master File
	        "%s/data/%s/oth%s.zip"   : "%s/data/%s/oth%s/",   # Any transaction from one committee to another
	        "%s/data/%s/pas2%s.zip"  : "%s/data/%s/pas2%s/",  # Contributions to candidates (and other expenditures) from committees
	        "%s/data/%s/indiv%s.zip" : "%s/data/%s/indiv%s/", # Contributions by individuals
	       }

  if not os.path.isdir("data"):
    os.mkdir("data")
  try:
    for year in range(start_year, end_year, 2):
      year_suffix = str(year)[2:]
      temp_files = files
      
      if year <= 1998:
        temp_files = files_1998
        
      for f in sorted(temp_files):
        archive = f % (cwd, year, year_suffix)
        extract_to = temp_files[f] % (cwd, year, year_suffix)
        if not os.path.isdir(extract_to):
          os.mkdir(extract_to)
        zf = zipfile.ZipFile(archive)
        print "Extracting %s to:\n   %s" % (archive, extract_to+zf.namelist()[0])
        zf.extractall(extract_to)
  except OSError:
    print "Have you run 'make download' yet?"
    os.sys.exit(1)
