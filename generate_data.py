#!/usr/bin/env python
''' This file generates sample data set for research and experimentation of 
detection of anomalous data absences
'''
__author__ = 'Edmon Begoli'

import sqlite3
import random
import csv
import sys
import getopt
from datetime import date, datetime
from random import randint

state_zip = {}
state_county_zip=[]
state_county_zip_dict={}
icds = []
#states = ('AL','AR','GA','NC','TN','SC', 'KY','FL','MO','VA','WV')
states = ["TN"]
zip_codes = []
ndc_codes = []


counties_to_exclude = ("Dyer", "Obion", "Carter")


def generate_claim(state,zip,hrr,provider_id):
	pass
	
def setup_hsa_hrr():
	''' Sets up Hospital Regions '''
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
	""" Creates a table with all icd-9 codes """
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

def setup_drug_codes():
	""" Creates a table with all drug codes """
	conn = sqlite3.connect( 'source.db' )
	curs = conn.cursor()
	curs.execute('DROP TABLE IF EXISTS ndc_product;')
	create_string = """CREATE TABLE ndc_product(id INTEGER PRIMARY KEY,
		product_id TEXT, name TEXT,	suffix TEXT,	non_proprietary_name TEXT, dea_schedule TEXT);"""
	curs.execute(create_string)
	reader = csv.reader(open('ndc_product.csv', 'rU'))
	for row in reader:
			to_db = [unicode(item, "utf8") for item in row] 
			insert_query = """ INSERT INTO ndc_product(product_id, name, suffix, non_proprietary_name, dea_schedule) VALUES (?,?,?,?,?); """
			curs.execute(insert_query, to_db)
			conn.commit()
	print 'done storing ndc codes!'		

def setup_states():
	""" Sets up a states database with all the cities, zips, etc. """
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


def random_claim_id(begin_id=0, end_id=99999):
	""" generates the random int representing the claim id in the default range between 0 and 99999 """
	return randint(begin_id, end_id)

def load_state_zips(state):
	""" loads all zips for all states """
	global state_zip
	with sqlite3.connect( 'source.db' ) as conn:
		cur = conn.cursor()
        state_zip[state] = [ row[0] for row in 
          cur.execute("SELECT zip FROM states WHERE state=:st",{"st":str(state)})]

def load_ndc():
	""" loads all drug codes"""
	global ndc_codes
	with sqlite3.connect( 'source.db' ) as conn:
		cur = conn.cursor()
        ndc_codes = [ map(str,row) for row in 
          cur.execute("SELECT product_id, name, suffix, non_proprietary_name, dea_schedule FROM ndc_product")]

def load_state_county_zips(state):
	""" loads all zips for all states """
	global state_county_zip
	with sqlite3.connect( 'source.db' ) as conn:
		cur = conn.cursor()
        state_county_zip = [ (str(row[0]),row[1]) for row in 
             cur.execute("SELECT county, zip FROM states WHERE state=:st AND county NOT NULL AND county <> '' ORDER BY county, zip DESC",{"st":str(state)})]

def random_date(begin=-30,end=0):
	""" returns a random date. default is the interval within the past 30 days"""
	start_date =  date.today().toordinal()+begin
	end_date = date.today().toordinal()+end
	random_date = date.fromordinal(random.randint(start_date, end_date))
	return random_date

def random_cost(low=1, high=150000):
	""" randomly returns the cost range, uniformly distributed"""
	return random.uniform(low, high)

def random_icd():
	""" returns randomly selected ICD """
	global icds
	if not icds:
		with sqlite3.connect( 'source.db' ) as conn:
			cur = conn.cursor()
			icds = [ row[0] for row in 
			cur.execute("SELECT code FROM icd9") ]
	return random.choice(icds) 

def random_drug_code():
	""" Returns random drug"""
	return random.choice(ndc_codes)

def random_state():
	""" Returns random state. """
	return random.choice(states)

def random_zip(state):
	""" randomly picks up the zip from the state """
	global state_zip
	if not state in state_zip:
		load_state_zips(state)
	return random.choice(state_zip[state])

def get_zip_county_dict():
	""" Creates a dictionary for county zips keyed by county """
	global state_county_zip_dict
	key = ""
	for item in state_county_zip:
		print item
		if item[0] not in key:
			key = item[0]
			state_county_zip_dict[key] = [item[1]]
		else:
			print key
			state_county_zip_dict[key].append(item[1])

def generate_dataset(max=50):
	""" generates a sample dataset """
	for i in range(max):
		state = random.choice(states)
		print (',').join( map(str,[random_claim_id(), state, random_zip(state), random_date(), random_date(), "%.2f" % random_cost(), random_icd() ])) 


def generate_day_map():
	begin = datetime.date(2008, 8, 15)
	end = datetime.date(2008, 9, 15)
	next_day = begin
	while True:
		if next_day > end:
			break
		print next_day
		next_day += datetime.timedelta(days=1)

def store_test_dataset( test_dataset, source='source.db' ):
	""" Tests the database connection. """
	conn = sqlite3.connect(location)
	c = conn.cursor()
	for item in test_dataset:
		c.execute('insert into test_dataset values (?,?,?)', item)
	conn.commit()

def main(argv):
	""" Main function of the program"""
	try:
		opts, args = getopt.getopt(argv,"hi:o:",["ifile="," p="])
	except getopt.GetoptError:
		print 'generate_data.py -zip -date -icd'
		sys.exit(2) 
	for opt, arg in opts:
		pass

def get_dataset_item():
	"""  Generates the random dataset item for the testing purposes. """
	state = random.choice(states)
	#return [str(item) for item in [random_claim_id(), state, random_zip(state), random_date(begin=-60,end=-5), random_date(begin=-30), "%.2f" % random_cost(), random_icd() ]]
	return [str(item) for item in [random_drug_code()[0], random_zip(state),random_date(begin=-160,end=-5)]]


if __name__ == '__main__':
	"""
	for i in range(10):
		print get_dataset_item()
	load_state_zips("TN")
	print state_zip	
	load_state_county_zips("TN")
	get_zip_county_dict()

	print "\n".join(map(str,state_county_zip))
	print state_county_zip_dict
	"""
	load_state_county_zips(random.choice(states))
	load_ndc()
	for i in range(10):
		print get_dataset_item()
	main(sys.argv[1:])


