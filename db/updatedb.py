#!/usr/bin/env python
import psycopg2
import os
import config
from connect import Connection
from datetime import datetime
from ftp.download_files import download_files
from ftp.extract_files import extract_files

class UpdateDB():
  '''Updates database entries from last update check'''
  def __init__(self):
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
    
    self.update_stmt = "UPDATE %s SET %s='%s' WHERE %s='%s';"
    self.insert_stmt = "INSERT INTO %s %s VALUES %s;"
    self.select_stmt = "SELECT %s FROM %s WHERE %s='%s';"
    self.download_files = download_files
    self.extract_files = extract_files
    self.cwd = config.cwd
    self.start_year = config.start_year
    self.end_year = config.end_year
  
  def update(self):
    self.download_files(self.start_year, self.end_year, self.cwd)
    self.extract_files(self.start_year, self.end_year, self.cwd)