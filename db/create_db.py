#!/usr/bin/env python
import psycopg2
import os
import sqlalchemy
from sqlalchemy import create_engine
from time import sleep

#http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCandidateMaster.shtml
candidate_master_sql = """CREATE TABLE candidate_master ( \
                                           ID SERIAL NOT NULL, \
                                           CAND_ID VARCHAR(14) NOT NULL, \
                                           CAND_NAME VARCHAR(250), \
                                           CAND_PTY_AFFILIATION VARCHAR(14), \
                                           CAND_ELECTION_YR SMALLINT, \
                                           CAND_OFFICE_ST VARCHAR(4), \
                                           CAND_OFFICE VARCHAR(1), \
                                           CAND_OFFICE_DISTRICT VARCHAR(4), \
                                           CAND_ICI VARCHAR(4), \
                                           CAND_STATUS VARCHAR(4), \
                                           CAND_PCC VARCHAR(14), \
                                           CAND_ST1 VARCHAR(40), \
                                           CAND_ST2 VARCHAR(40), \
                                           CAND_CITY VARCHAR(35), \
                                           CAND_ST VARCHAR(5), \
                                           CAND_ZIP VARCHAR(14));"""

#http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCommitteeMaster.shtml
commitee_master_sql = """CREATE TABLE committee_master ( \
                                           ID SERIAL NOT NULL, \
                                           CMTE_ID VARCHAR(14) NOT NULL,\
                                           CMTE_NM VARCHAR(250), \
                                           TRES_NM VARCHAR(100), \
                                           CMTE_ST1 VARCHAR(40), \
                                           CMTE_ST2 VARCHAR(40), \
                                           CMTE_CITY VARCHAR(35), \
                                           CMTE_ST VARCHAR(4), \
                                           CMTE_ZIP VARCHAR(14), \
                                           CMTE_DSGN VARCHAR(4), \
                                           CMTE_TP VARCHAR(4), \
                                           CMTE_PTY_AFFILIATION VARCHAR(5), \
                                           CMTE_FILING_FREQ VARCHAR(4), \
                                           ORG_TP VARCHAR(4), \
                                           CONNECTED_ORG_NM VARCHAR(250), \
                                           CAND_ID VARCHAR(14));"""

#http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCandCmteLinkage.shtml                        
candidate_linkage_sql = """CREATE TABLE candidate_linkage ( \
                                           ID SERIAL NOT NULL, \
                                           CAND_ID VARCHAR(14) NOT NULL, \
                                           CAND_ELECTION_YR SMALLINT, \
                                           FEC_ELECTION_YR SMALLINT, \
                                           CMTE_ID VARCHAR(14), \
                                           CMTE_TP VARCHAR(4), \
                                           CMTE_DSGN VARCHAR(4), \
                                           LINKAGE_ID BIGINT);"""
                                           
#http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCommitteetoCommittee.shtml
comm_to_comm_sql = """CREATE TABLE comm_to_comm ( \
                                        ID SERIAL NOT NULL, \
                                        CMTE_ID VARCHAR(14), \
                                        AMNDT_IND VARCHAR(4), \
                                        RPT_TP VARCHAR(5), \
                                        TRANSACTION_PGI VARCHAR(10), \
                                        IMAGE_NUM VARCHAR(15), \
                                        TRANSACTION_TP VARCHAR(5), \
                                        ENTITY_TP VARCHAR(5), \
                                        NAME VARCHAR(250), \
                                        CITY VARCHAR(35), \
                                        STATE VARCHAR(5), \
                                        ZIP_CODE VARCHAR(14), \
                                        EMPLOYER VARCHAR(50), \
                                        OCCUPATION VARCHAR(50), \
                                        TRANSACTION_DT DATE, \
                                        TRANSACTION_AMT INTEGER, \
                                        OTHER_ID VARCHAR(14), \
                                        TRAN_ID VARCHAR(35), \
                                        FILE_NUM VARCHAR(25), \
                                        MEMO_CD VARCHAR(5), \
                                        MEMO_TEXT VARCHAR(100), \
                                        SUB_ID BIGINT UNIQUE NOT NULL);"""
                                        
# http://www.fec.gov/finance/disclosure/metadata/DataDictionaryContributionstoCandidates.shtml
cand_to_comm_sql = """CREATE TABLE cand_to_comm ( \
                                      ID SERIAL NOT NULL, \
                                      CMTE_ID VARCHAR(14) NOT NULL, \
                                      AMNDT_IND VARCHAR(5), \
                                      RPT_TP VARCHAR(5), \
                                      TRANSACTION_PGI VARCHAR(10), \
                                      IMAGE_NUM VARCHAR(15), \
                                      TRANSACTION_TP VARCHAR(5), \
                                      ENTITY_TP VARCHAR(5), \
                                      NAME VARCHAR(250), \
                                      CITY VARCHAR(35), \
                                      STATE VARCHAR(5), \
                                      ZIP_CODE VARCHAR(14), \
                                      EMPLOYER VARCHAR(50), \
                                      OCCUPATION VARCHAR(50), \
                                      TRANSACTION_DT DATE, \
                                      TRANSACTION_AMT INTEGER, \
                                      OTHER_ID VARCHAR(14), \
                                      CAND_ID VARCHAR(14), \
                                      TRAN_ID VARCHAR(35), \
                                      FILE_NUM VARCHAR(25), \
                                      MEMO_CD VARCHAR(5), \
                                      MEMO_TEXT VARCHAR(100), \
                                      SUB_ID BIGINT UNIQUE NOT NULL);"""

#http://www.fec.gov/finance/disclosure/metadata/DataDictionaryContributionsbyIndividuals.shtml
individual_contrib_sql = """CREATE TABLE indiv_contrib ( \
                                          CMTE_ID VARCHAR(14) NOT NULL, \
                                          AMNDT_IND VARCHAR(5), \
                                          RPT_TP VARCHAR(5), \
                                          TRANSACTION_PGI VARCHAR(9), \
                                          IMAGE_NUM VARCHAR(15), \
                                          TRANSACTION_TP VARCHAR(5), \
                                          ENTITY_TP VARCHAR(5), \
                                          NAME VARCHAR(250), \
                                          CITY VARCHAR(35), \
                                          STATE VARCHAR(5), \
                                          ZIP_CODE VARCHAR(14), \
                                          EMPLOYER VARCHAR(50), \
                                          OCCUPATION VARCHAR(50), \
                                          TRANSACTION_DT DATE, \
                                          TRANSACTION_AMT INTEGER, \
                                          OTHER_ID VARCHAR(14), \
                                          TRAN_ID VARCHAR(35), \
                                          FILE_NUM VARCHAR(25), \
                                          MEMO_CD VARCHAR(5), \
                                          MEMO_TEXT VARCHAR(100), \
                                          SUB_ID BIGINT UNIQUE NOT NULL);"""
def create_db(start_year, end_year, cwd, db_prefix, db_user, db_password, db_host, db_port):       
  """Creates databases and populates those databases with the templates provided by config.py"""
  for year in range(start_year, end_year, 2):
    try:
      conn = psycopg2.connect(database=db_prefix.lower()+str(year),
			      user=db_user,
			      password=db_password,
			      host=db_host,
			      port=db_port
			     )
      conn.set_client_encoding("UTF8")
      cur = conn.cursor()
    
    except psycopg2.OperationalError as e:
      try:
        print "Database %s%s does not exist yet, creating now" % (db_prefix.lower(), year)
        engine_stmt = 'postgresql+psycopg2://%s:%s@%s:%s/template1' % \
	              (db_user, db_password, db_host, db_port)
        engine = create_engine(engine_stmt)
        eng_conn = engine.connect()
        eng_conn.connection.connection.set_isolation_level(0)
        create_db_stmt = "CREATE DATABASE %s%s" % (db_prefix.lower(), year)
        eng_conn.execute(create_db_stmt)
        eng_conn.connection.connection.set_isolation_level(1)
        eng_conn.close()
        engine.dispose()
        sleep(1)
      except sqlalchemy.exc.ProgrammingError as e:
	print "Error:%s\nDid you modify your configuration file and run make createuser?" % e
	os.sys.exit(1)
      conn = psycopg2.connect(database=db_prefix.lower()+str(year),
	  	              user=db_user,
			      password=db_password,
			      host=db_host,
			      port=db_port
			      )
      conn.set_client_encoding("UTF8")
      cur = conn.cursor()
      
    
  
    if year <= 1998:
    
      cur.execute("""DROP TABLE IF EXISTS candidate_master;""")
      cur.execute(candidate_master_sql)
      cur.execute("""DROP TABLE IF EXISTS committee_master;""")
      cur.execute(commitee_master_sql)
      cur.execute("""DROP TABLE IF EXISTS comm_to_comm;""")
      cur.execute(comm_to_comm_sql)
      cur.execute("""DROP TABLE IF EXISTS cand_to_comm;""")
      cur.execute(cand_to_comm_sql)
      cur.execute("""DROP TABLE IF EXISTS indiv_contrib;""")
      cur.execute(individual_contrib_sql)
    
    else:
    
      cur.execute("""DROP TABLE IF EXISTS candidate_master;""")
      cur.execute(candidate_master_sql)
      cur.execute("""DROP TABLE IF EXISTS committee_master;""")
      cur.execute(commitee_master_sql)
      cur.execute("""DROP TABLE IF EXISTS candidate_linkage;""")
      cur.execute(candidate_linkage_sql)
      cur.execute("""DROP TABLE IF EXISTS comm_to_comm;""")
      cur.execute(comm_to_comm_sql)
      cur.execute("""DROP TABLE IF EXISTS cand_to_comm;""")
      cur.execute(cand_to_comm_sql)
      cur.execute("""DROP TABLE IF EXISTS indiv_contrib;""")
      cur.execute(individual_contrib_sql)
    
    conn.commit()
    cur.close()
    conn.close()
    sleep(1)
  


                                          