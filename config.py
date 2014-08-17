#!/usr/bin/env python

##########################################
## MODIFY THIS FILE TO YOUR PREFERENCES ##
##########################################

# Start year to maintain data from (must be >= 1998) default is 1998
# Can be 1998, 2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014 ..

start_year = 1998

# End year to maintain data to, must be >= start_year+1, default is 2015
# can be 1999, 2001, 2003, 2005, 2007, 2009, 2011, 2013, 2015 ...

end_year = 2015

# Database user *** THIS MUST BE CHANGED AND CANNOT BE EMPTY ***

db_user = ""

# Database password, *** THIS MUST NOT BE EMPTY ***

db_password = ""

# DB Prefix, default = fec_
# must conform to valid identifier/keyword rules for postgresql
# for more information, see http://www.postgresql.org/docs/current/interactive/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
# will be converted to lowercase when database is created for normalization purposes

db_prefix = "fec_"

# DB Host, default = localhost

db_host = "localhost"

# DB Port, default = 5432

db_port = 5432

## DO NOT MODIFY BELOW THIS LINE
import os

cwd = os.getcwd()

