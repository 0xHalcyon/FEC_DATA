all: download extract createdb populatedb



download:
	./ftp/download_files.py

extract:
	./ftp/extract_files.py

createdb:
	./db/create_db.py

populatedb:
	./db/populate_database.py