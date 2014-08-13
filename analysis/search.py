#!/usr/bin/env python
import pandas
import sys
from sqlalchemy import create_engine


def search_names_geo(field, query, year, db_prefix, db_user, db_password, db_host, db_port):
  engine_stmt = 'postgresql+psycopg2://%s:%s@%s:%s/%s%s' % \
                (db_user, db_password, db_host, db_port, db_prefix, str(year))
  engine = create_engine(engine_stmt)
  if year <= 1998:
    candidates_stmt = "SELECT * FROM candidate_master WHERE {0} LIKE '%%{1}%%';".format(field, query.upper())
    names = pandas.read_sql(candidates_stmt, engine)
    
  