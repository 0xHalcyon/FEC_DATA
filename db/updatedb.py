#!/usr/bin/env python
import psycopg2
import os
from datetime import datetime
from ftp import download_files
from ftp import extract_files
class UpdateDB():
  '''Updates database entries from last update check'''
  def __init__(self, Connection):
    self.__Connection = Connection
    self.files = {
                    "committee_master"  : ["%s/data/%s/cm%s/cm.txt", "%s/db/headers/cm_header_file.csv"],
                    "candidate_master"  : ["%s/data/%s/cn%s/cn.txt", "%s/db/headers/cn_header_file.csv"],
                    "candidate_linkage" : ["%s/data/%s/ccl%s/ccl.txt", "%s/db/headers/ccl_header_file.csv"],
                    "comm_to_comm"      : ["%s/data/%s/oth%s/itoth.txt","%s/db/headers/oth_header_file.csv"],
                    "cand_to_comm"      : ["%s/data/%s/pas2%s/itpas2.txt", "%s/db/headers/pas2_header_file.csv"],
                    "indiv_contrib"     : ["%s/data/%s/indiv%s/itcont.txt", "%s/db/headers/indiv_header_file.csv"]
                 }

    self.files_1998 = {
                         "committee_master"  : ["%s/data/%s/cm%s/cm.txt", "%s/db/headers/cm_header_file.csv"],
                         "candidate_master"  : ["%s/data/%s/cn%s/cn.txt", "%s/db/headers/cn_header_file.csv"],
                         "comm_to_comm"      : ["%s/data/%s/oth%s/itoth.txt","%s/db/headers/oth_header_file.csv"],
                         "cand_to_comm"      : ["%s/data/%s/pas2%s/itpas2.txt", "%s/db/headers/pas2_header_file.csv"],
                         "indiv_contrib"     : ["%s/data/%s/indiv%s/itcont.txt", "%s/db/headers/indiv_header_file.csv"]
                      }
    
    self.download_files = download_files
    self.extract_files = extract_files
    self.cwd = os.getcwd()
  
  def updatedb(self, start_year, end_year):
    
    pass