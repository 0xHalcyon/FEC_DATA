#!/usr/bin/python

from ftplib import FTP

ftp =FTP("ftp.fec.gov")

ftp.login()

ftp.cwd('FEC')

ftp.retrlines("LIST")

