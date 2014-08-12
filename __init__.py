#!/usr/bin/env python

import os
import config
import sys
#db.create_db.create_db(start_year, end_year, cwd, db_prefix, db_user, db_password, db_host, db_port)
#db.populate_database.populate_database(start_year, end_year, cwd, db_prefix, db_user, db_password, db_host, db_port)
#ftp.download_files.download_files(start_year, end_year, cwd)
#ftp.extract_files.extract_files(start_year, end_year, cwd)


def wrapper(function, cwd):
  
  if function == "download":  
    
    from ftp import download_files
    download_files.download_files(config.start_year,
				  config.end_year, cwd)
    
  elif function == "extract":
    
    from ftp import extract_files
    extract_files.extract_files(config.start_year,
				config.end_year, cwd)
    
  elif function == "createdb":
    
    from db import create_db
    create_db.create_db(config.start_year, 
			config.end_year,
			cwd,
			config.db_prefix,
			config.db_user, 
			config.db_password,
		        config.db_host, 
		        config.db_port)
    
  elif function == "populatedb":
    
    from db import populate_database
    populate_database.populate_database(config.start_year,
					config.end_year,
					cwd,
					config.db_prefix,
					config.db_user,
					config.db_password,
				        config.db_host,
				        config.db_port)
  elif function == "createuser":
    print "Will now create new user in PostgresSQL"
    print "At the prompt, enter the password for the new user, as set in config.py"
    os.system("""createuser --createdb --encrypted --pwprompt --no-superuser --no-createrole --host=%s --port=%s --username=postgres --password %s""" % \
	       (config.db_host, config.db_port, config.db_user))

  elif function == "clean_uninstall":
    pass
  
  else:
    print "Invalid function: %s" % function
  
if __name__ == "__main__":
  workDir = os.getcwd()
  try:
    toDo = sys.argv[1]
  except IndexError:
    print "Usage: %s <function>" % sys.argv[0]
    sys.exit(1)
  wrapper(toDo, workDir)
  
  