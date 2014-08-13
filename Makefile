all: download extract createdb populatedb



download:
	./__init__.py download

extract:
	./__init__.py extract

createdb:
	./__init__.py createdb

populatedb:
	./__init__.py populatedb
	
createuser:
	./__init__.py createuser
	
creategeo:
	./__init__.py creategeo