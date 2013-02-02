#!/usr/bin/env python
''' This file generates sample data set for research and experimentation of 
detection of anomalous data absences
'''
__author__ = 'Edmon Begoli'

import sqlite3
import random
import csv
from datetime import date, datetime
from random import randint

state_zip = {}
icds = []
states = ('AL','AR','GA','NC','TN','SC', 'FL','MO','VA','WV')

def generate_claim(state,zip,hrr,provider_id):
	pass
	
def setup_hsa_hrr():
	''' Sets up Hospital Regios '''
	conn = sqlite3.connect( 'source.db' )
	curs = conn.cursor()
	curs.execute('DROP TABLE IF EXISTS zip_hsa_hrr;')
	create_string = """CREATE TABLE zip_hsa_hrr (id INTEGER PRIMARY KEY,
		zipcode10 INTEGER,hsanum INTEGER,hsacity TEXT,hsastate TEXT,hrrnum INTEGER,
		hrrcity TEXT,hrrstate TEXT);"""
	curs.execute(create_string)
	reader = csv.reader(open('zip_hsa_hrr2010.csv', 'rU'))
	first = True
	for row in reader:
		if not first:
			to_db = [int(row[0]), int(row[1]),unicode(row[2],"utf8"),
			 unicode(row[3],"utf8"), int(row[4]), 
			  unicode(row[5], "utf8"),unicode(row[6],"utf8")] 
			insert_query = """ INSERT INTO zip_hsa_hrr (zipcode10,hsanum,hsacity,hsastate,
				hrrnum,hrrcity,hrrstate) 
              VALUES (?,?,?,?,?,?,?); """
			curs.execute(insert_query, to_db)
			conn.commit()
		else:
			first = False
	print 'done storing hrr-hsa zip crosswalk!'

def setup_codes():
	conn = sqlite3.connect( 'source.db' )
	curs = conn.cursor()
	curs.execute('DROP TABLE IF EXISTS icd9;')
	create_string = """CREATE TABLE icd9 (id INTEGER PRIMARY KEY,
		code TEXT,short_desc TEXT, long_desc TEXT);"""
	curs.execute(create_string)
	reader = csv.reader(open('icd9.csv', 'rU'))
	first = True
	for row in reader:
		if not first:
			to_db = [unicode(row[0], "utf8"), unicode(row[1], "utf8"),unicode(row[2],"utf8")] 
			insert_query = """ INSERT INTO icd9 (code,short_desc,long_desc) VALUES (?, ?, ?); """
			curs.execute(insert_query, to_db)
			conn.commit()
		else:
			first = False
	print 'done storing icd9 codes!'	

def setup_states():
	conn = sqlite3.connect( 'source.db' )
	curs = conn.cursor()
	curs.execute('DROP TABLE IF EXISTS states;')
	create_string = """CREATE TABLE states (id INTEGER PRIMARY KEY,zip INTEGER,
		type TEXT,primary_city TEXT, acceptable_cities TEXT, unacceptable_cities TEXT,	
		state TEXT,	county TEXT, timezone TEXT,	area_codes TEXT,	latitude REAL,	
		longitude REAL,	world_region TEXT,country TEXT,decommissioned TEXT,
		estimated_population REAL, notes TEXT);"""
	curs.execute(create_string)
	reader = csv.reader(open('zip_code_database.csv', 'rU'))
	first = True
	for row in reader:
		if not first:
			to_db = [long(row[0]),unicode(row[1], "utf8"), unicode(row[2], "utf8"),
			unicode(row[3], "utf8"),unicode(row[4], "utf8"), unicode(row[5], "utf8"),
			unicode(row[6], "utf8"), unicode(row[7], "utf8"),unicode(row[8],"utf8"),
			float(row[9]),float(row[10]), unicode(row[11],"utf8"),unicode(row[12], "utf8"), 
			unicode(row[13], "utf8"),float(row[14]),unicode(row[15], "utf8")] 
			insert_query = """ INSERT INTO states (zip,type,primary_city, acceptable_cities, unacceptable_cities, state, county, timezone,area_codes,latitude, longitude, world_region,country,decommissioned, estimated_population, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?); """
			curs.execute(insert_query, to_db)
			conn.commit()
		else:
			first = False
	print 'done storing state data!'

def random_claim_id():
	return randint(10000, 99999)

def load_state_zips(state):
	global state_zip
	with sqlite3.connect( 'source.db' ) as conn:
		cur = conn.cursor()
        state_zip[state] = [ row[0] for row in 
          cur.execute("SELECT zip FROM states WHERE state=:st",{"st":str(state)}) ]


#TODO: allow to specify start date and end date
def random_date(my_year=datetime.now().year):
	start_date = date(day=1, month=1, year=my_year).toordinal()
	end_date = date(day=31, month=12, year=my_year).toordinal()
	random_day = date.fromordinal(random.randint(start_date, end_date))
	return random_day

def random_cost(low=500, high=15000):
	return random.uniform(low, high)

def random_icd():
	global icds
	if not icds:
		with sqlite3.connect( 'source.db' ) as conn:
			cur = conn.cursor()
			icds = [ row[0] for row in 
			cur.execute("SELECT code FROM icd9") ]
	return icds[randint(0,len(icds))] 

def random_zip(state):
	global state_zip
	if not state in state_zip:
		load_state_zips(state)

	return state_zip[state][randint( 0,len(state_zip[state]) )]

def generate_dataset():
	for state in states:
		print (',').join( map(str,[random_claim_id(), state, random_zip(state), random_date(), random_date(), "%.2f" % random_cost(), random_icd() ])) 


def store_test_dataset( test_dataset, source='source.db' ):
	conn = sqlite3.connect(location)
	c = conn.cursor()
	for item in test_dataset:
		c.execute('insert into test_dataset values (?,?,?)', item)
	conn.commit()

if __name__ == '__main__':
	for i in range(10):
		print states[i%5]
		print random_date()
		print random_zip(states[i%5])
		print "%.2f" % random_cost() 
		print random_icd()
	generate_dataset()


