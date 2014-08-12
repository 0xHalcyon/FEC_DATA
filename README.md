FEC_DATA
========

Maintains a PostgresSQL database of the FEC's FTP data, and some limited scripts for data retrieval.

SETUP
=====

First edit config.py to your preferences, the file is documented.


INIT
====

Now run 'make createuser'

You will be prompted as shown below:

Enter password for new role: 
Enter it again: 

These two fields are for the new database user you listed in config.py

Now you will be prompted for your postgresql password. If none, just hit enter.
The command will exit successfully if no further text appears on the screen. 

You may see text like so:
createuser: creation of new role failed: ERROR:  role "foo" already exists

This can be ignored.

Now, run 'make'. This will download and extract the files from the FEC's
FTP website (ftp://ftp.fec.gov), then will create new databases based on the 
information provided in your config.py file, and populate those databases with the 
information downloaded from the FEC's website. 

Errors will be stored in ./db/errors/ if any occur. 


More to come!

