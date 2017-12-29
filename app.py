#############################################
# Dependencies
#############################################
# Flask (Server)
from flask import Flask, jsonify, render_template, request

# Sql Alchemy (ORM)
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy import exc

# Various
import datetime as dt
from random import *
import json
import sys

# Dependencies
import os
import pandas as pd
import numpy as np


#############################################
# Database Setup
#############################################
# Connection String
engine = create_engine("sqlite:///db/belly_button_biodiversity.sqlite")

# Reflect DB Contents using SQL Alchmemy
Base = automap_base()
Base.prepare(engine, reflect=True)

# Store each table as a class to make it available on the python side
otu_table = Base.classes.otu
samples_table = Base.classes.samples
metadata_table = Base.classes.samples_metadata


#############################################
# Flask Setup
#############################################
app = Flask(__name__)

#############################################
# Flask Routes (Web)
#############################################
@app.route('/')
def basic():
	return render_template('index.html')

#route for names of samples
@app.route('/names')
def names():
	#connect to the DB
	session = Session(engine)

	#get list of sample names

	sample_query = session.query(samples_table).statement
	df = pd.read_sql_query(sample_query, session.bind)
	df.set_index('otu_id', inplace=True)

	return jsonify(list(df.columns))

# Query routes
@app.route('/otu')
def otu_query():
	#connect to the DB
	session = Session(engine)

	#get all otus
	results = session.query(otu_table)

	#specify to get all results
	results = results.all()

	#convert results into dictionary so we can use it as a json
	# create list to hold results
	all_results = []

	for result in results:
		dict_results = {}

		dict_results["otu_id"] = result.otu_id
		dict_results["lowest_taxonomic_unit_found"] = result.lowest_taxonomic_unit_found

		#append the individual results into the array
		all_results.append(dict_results)

	return(jsonify(all_results))

@app.route('/metadata/<sample>')
def metadata(sample):
	#connect to the DB
	session = Session(engine)

	sel = [metadata_table.SAMPLEID, metadata_table.ETHNICITY,
			metadata_table.GENDER, metadata_table.AGE,
			metadata_table.LOCATION, metadata_table.BBTYPE]

	# remove the BB prefix by seeing if it matches after the BB
	results = session.query(*sel).\
	filter(metadata_table.SAMPLEID == sample[3:]).all()

	sample_meta = {}

	# loop through the results and assign it the value in ordr it comes
	# from the sqlite database
	for result in results:
		sample_meta['SAMPLEID'] = result[0]
		sample_meta['ETHNICITY'] = result[1]
		sample_meta['GENDER'] = result[2]
		sample_meta['AGE'] = result[3]
		sample_meta['LOCATION'] = result[4]
		sample_meta['BBTYPE'] = result[5]

	return jsonify(sample_meta)

@app.route('/wfreq/<sample>')
def samples_wfreq(sample):
	#connect to the DB
	session = Session(engine)

	#washing frequencies as number

	#strip BB like we did last time
	results = session.query(metadata_table.WFREQ).\
	filter(metadata_table.SAMPLEID == sample[3:]).all()
	wfreq = np.ravel(results)

	# Return only the first integer value for washing frequency
	return jsonify(int(wfreq[0]))


@app.route('/samples/<sample>')
def samples(sample):

	#connect to the DB
	session = Session(engine)	
	#return a list of dictionaries containgin the otu_ids and sample_values
	sample_quer = session.query(samples_table).statement
	df = pd.read_sql_query(sample_quer, session.bind)

	#check to see if sample was found
	if sample not in df.columns:
		return jsonify(f"Error, {sample} not found"), 400

	#return only greater than 1 	
	df = df[df[sample] > 1]

	#sort 
	df = df.sort_values(by=sample, ascending=0)

	#format to jason
	data = [{

		"otu_ids": df[sample].index.values.tolist(),
		"sample_values": df[sample].values.tolist()
	}]
	return jsonify(data)

#############################################
# Default App Settings
#############################################

if __name__ == "__main__":
	app.run(debug=True)
