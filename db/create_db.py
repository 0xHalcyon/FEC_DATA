#!/usr/bin/env python
import psycopg2

#http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCandidateMaster.shtml
candidate_master_sql = """CREATE TABLE candidate_master ( \
                                           ID SERIAL NOT NULL, \
                                           CAND_ID VARCHAR(9) NOT NULL, \
                                           CAND_NAME VARCHAR(200), \
                                           CAND_PTY_AFFILIATION VARCHAR(3), \
                                           CAND_ELECTION_YR SMALLINT, \
                                           CAND_OFFICE_ST VARCHAR(2), \
                                           CAND_OFFICE VARCHAR(1), \
                                           CAND_OFFICE_DISTRICT VARCHAR(2), \
                                           CAND_ICI VARCHAR(1), \
                                           CAND_STATUS VARCHAR(1), \
                                           CAND_PCC VARCHAR(9), \
                                           CAND_ST1 VARCHAR(34), \
                                           CAND_ST2 VARCHAR(34), \
                                           CAND_CITY VARCHAR(30), \
                                           CAND_ST VARCHAR(2), \
                                           CAND_ZIP VARCHAR(9));"""

#http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCommitteeMaster.shtml
commitee_master_sql = """CREATE TABLE committee_master ( \
                                           ID SERIAL NOT NULL, \
                                           CMTE_ID VARCHAR(9) NOT NULL,\
                                           CMTE_NM VARCHAR(200), \
                                           TRES_NM VARCHAR(90), \
                                           CMTE_ST1 VARCHAR(34), \
                                           CMTE_ST2 VARCHAR(34), \
                                           CMTE_CITY VARCHAR(30), \
                                           CMTE_ST VARCHAR(2), \
                                           CMTE_ZIP VARCHAR(9), \
                                           CMTE_DSGN VARCHAR(1), \
                                           CMTE_TP VARCHAR(1), \
                                           CMTE_PTY_AFFILIATION VARCHAR(3), \
                                           CMTE_FILING_FREQ VARCHAR(1), \
                                           ORG_TP VARCHAR(1), \
                                           CONNECTED_ORG_NM VARCHAR(200), \
                                           CAND_ID VARCHAR(9));"""

#http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCandCmteLinkage.shtml                        
candidate_linkage_sql = """CREATE TABLE candidate_linkage ( \
                                           ID SERIAL NOT NULL, \
                                           CAND_ID VARCHAR(9) NOT NULL, \
                                           CAND_ELECTION_YR SMALLINT, \
                                           FEC_ELECTION_YEAR SMALLINT, \
                                           CMTE_ID VARCHAR(9), \
                                           CMTE_TP VARCHAR(1), \
                                           CMTE_DSGN VARCHAR(1), \
                                           LINKAGE_ID BIGINT);"""
                                           
#http://www.fec.gov/finance/disclosure/metadata/DataDictionaryCommitteetoCommittee.shtml
comm_to_comm_sql = """CREATE TABLE comm_to_comm ( \
                                        ID SERIAL NOT NULL, \
                                        CMTE_ID VARCHAR(9), \
                                        AMNDT_IND VARCHAR(1), \
                                        RPT_TP VARCHAR(3), \
                                        TRANSACTION_PGI VARCHAR(5), \
                                        IMAGE_NUM VARCHAR(11), \
                                        TRANSACTION_TP VARCHAR(3), \
                                        ENTITY_TP VARCHAR(3), \
                                        NAME VARCHAR(200), \
                                        CITY VARCHAR(30), \
                                        STATE VARCHAR(2), \
                                        ZIP_CODE VARCHAR(9), \
                                        EMPLOYER VARCHAR(50), \
                                        OCCUPATION VARCHAR(50), \
                                        TRANSACTION_DT DATE, \
                                        TRANSACTION_AMT INTEGER, \
                                        OTHER_ID VARCHAR(9), \
                                        TRAN_ID VARCHAR(32), \
                                        FILE_NUM VARCHAR(22), \
                                        MEMO_CD VARCHAR(1), \
                                        MEMO_TEXT VARCHAR(100), \
                                        SUB_ID BIGINT UNIQUE NOT NULL);"""
                                        
# http://www.fec.gov/finance/disclosure/metadata/DataDictionaryContributionstoCandidates.shtml
cand_to_comm_sql = """CREATE TABLE cand_to_comm ( \
                                      ID SERIAL NOT NULL, \
                                      CMTE_ID VARCHAR(9) NOT NULL, \
                                      AMNDT_ID VARCHAR(1), \
                                      RPT_TP VARCHAR(3), \
                                      TRANSACTION_PGI VARCHAR(5), \
                                      IMAGE_NUM VARCHAR(11), \
                                      TRANSACTION_TP VARCHAR(3), \
                                      ENTITY_TP VARCHAR(3), \
                                      NAME VARCHAR(200), \
                                      CITY VARCHAR(30), \
                                      STATE VARCHAR(2), \
                                      ZIP_CODE VARCHAR(9), \
                                      EMPLOYER VARCHAR(50), \
                                      OCCUPATION VARCHAR(50), \
                                      TRANSACTION_DT DATE, \
                                      TRANSACTION_AMT INTEGER, \
                                      OTHER_ID VARCHAR(9), \
                                      CAND_ID VARCHAR(9), \
                                      TRAN_ID VARCHAR(32), \
                                      FILE_NUM VARCHAR(22), \
                                      MEMO_CD VARCHAR(1), \
                                      MEMO_TEXT VARCHAR(100), \
                                      SUB_ID BIGINT UNIQUE NOT NULL);"""

#http://www.fec.gov/finance/disclosure/metadata/DataDictionaryContributionsbyIndividuals.shtml
individual_contrib_sql = """CREATE TABLE indiv_contrib ( \
                                          CMTE_ID VARCHAR(9) NOT NULL, \
                                          AMNDT_IND VARCHAR(1), \
                                          RPT_TP VARCHAR(3), \
                                          TRANSACTION_PGI VARCHAR(5), \
                                          IMAGE_NUM VARCHAR(11), \
                                          TRANSACTION_TP VARCHAR(3), \
                                          ENTITY_TP VARCHAR(3), \
                                          NAME VARCHAR(200), \
                                          CITY VARCHAR(30), \
                                          STATE VARCHAR(2), \
                                          ZIP_CODE VARCHAR(9), \
                                          EMPLOYER VARCHAR(50), \
                                          OCCUPATION VARCHAR(50), \
                                          TRANSACTION_DT DATE, \
                                          TRANSACTION_AMT INTEGER, \
                                          OTHER_ID VARCHAR(9), \
                                          TRAN_ID VARCHAR(32), \
                                          FILE_NUM VARCHAR(22), \
                                          MEMO_CD VARCHAR(1), \
                                          MEMO_TEXT VARCHAR(100), \
                                          SUB_ID BIGINT UNIQUE NOT NULL);"""
for i in range(2004, 2015, 2):
  conn = psycopg2.connect(dbname="FEC_%s"%i, user="postgres")
  cur = conn.cursor()
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
  

                                          