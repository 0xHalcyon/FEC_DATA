#!/usr/bin/env python

import os
import config
import sys
import time
#db.create_db.create_db(start_year, end_year, cwd, db_prefix, db_user, db_password, db_host, db_port)
#db.populate_database.populate_database(start_year, end_year, cwd, db_prefix, db_user, db_password, db_host, db_port)
#ftp.download_files.download_files(start_year, end_year, cwd)
#ftp.extract_files.extract_files(start_year, end_year, cwd)


def wrapper(function):
  
  if function == "download":  
    
    from ftp import download_files
    download_files.download_files(config.start_year,
				  config.end_year, config.cwd)
    
  elif function == "extract":
    
    from ftp import extract_files
    extract_files.extract_files(config.start_year,
				config.end_year, config.cwd)
    
  elif function == "createdb":
    
    from db import create_db
    create_db.create_db(config.start_year, 
			config.end_year,
			config.cwd,
			config.db_name,
			config.db_user, 
			config.db_password,
		        config.db_host, 
		        config.db_port)
    
  elif function == "populatedb":
    conn_settings = {'db_password': config.db_password, 
	             'db_user': config.db_user,
	             'db_host': config.db_host,
	             'db_port': config.db_port,
	             'db_name': config.db_name,
	             'start_year': config.start_year,
	             'end_year': config.end_year
	            }
    from db.connect import Connection
    connections = Connection(conn_settings)
    from db import populate_database
    threads = {}
    for year in range(config.start_year, config.end_year, 2):
      threads[year] = populate_database.threadedPopulate(year, year, config.cwd, connections)
    for thread in sorted(threads):
      threads[thread].start()
    while True:
      for thread in sorted(threads):
	if all(None == x for x in threads.values()):
	  print "Complete"
	  connections.closeall()
	  return
	elif threads[thread] == None:
	  continue
	elif threads[thread].complete:
	  print "Thread: %s is complete" % threads[thread].name 
	  threads[thread].join()
	  print "Thread: %s is joined" % threads[thread].name
	  threads[thread] = None
	else:
	  time.sleep(1)
	  continue

  elif function == "createuser":
    print "Will now create new user in PostgresSQL"
    print "At the prompt, enter the password for the new user, as set in config.py"
    os.system("""createuser --createdb --encrypted --pwprompt --no-superuser --no-createrole --host=%s --port=%s --username=postgres --password %s""" % \
	       (config.db_host, config.db_port, config.db_user))

  elif function == "creategeo":
    from geo import geodatadb
    print "Creating zipcode database and dumping json to database"
    geodatadb.gengeodb(config.cwd, 
		       config.db_name, 
		       config.db_user, 
		       config.db_password, 
		       config.db_host, 
		       config.db_port)

  elif function == "clean_uninstall":
    pass
  
  else:
    print "Invalid function: %s" % function
  
if __name__ == "__main__":
  try:
    toDo = sys.argv[1]
  except IndexError:
    print "Usage: %s <function>" % sys.argv[0]
    sys.exit(1)
  wrapper(toDo)
  
  