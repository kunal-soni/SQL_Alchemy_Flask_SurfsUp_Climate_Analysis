# 1. import Flask, engine
#import pandas as pd
#import datetime as dt
import numpy as np
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, MetaData
from sqlalchemy.pool import StaticPool
from flask import Flask, jsonify
import logging
import datetime as dt
import re
from operator import itemgetter

# 2. Create an app, being sure to pass __name__
app = Flask(__name__)

# 3. Define what to do when a user hits the index route
@app.route("/")
def welcome():
    return (
        f"Welcome to the Climate APP API!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/about<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end><br/>"
    )

# 5. Define static routes
@app.route("/api/v1.0/about")
def about():
    name = "Kunal"
    location = "Orlando"

    return f"My name is {name}, and I live in {location}."

# Convert the query results to a Dictionary using date as the key and prcp as the value.
#################################################
# Database Setup
#################################################
# Web sites use threads, but sqlite is not thread-safe.
# These parameters will let us get around it.
# However, it is recommended you create a new Engine, Base, and Session
#   for each thread (each route call gets its own thread)
engine = create_engine("sqlite:///Resources/hawaii.sqlite",
    connect_args={'check_same_thread':False},
    poolclass=StaticPool)
# Create our session (link) from Python to the DB
session = Session(engine)
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)
# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# 6. Define function routes
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Calculate the date 1 year ago from the last data point in the database
    last_data_point_query = session.query(func.max(Measurement.date))
    for record in last_data_point_query:
        last_date = record 
        print(last_date)

    lastdatestring = str(last_date)
    print(lastdatestring)
    match = re.search('\d{4}-\d{2}-\d{2}', lastdatestring)
    last_date = dt.datetime.strptime(match.group(), '%Y-%m-%d').date()
    print(last_date)

    yearback = last_date - dt.timedelta(days=365)
    print(yearback)

    #date_input = input("Enter a date between: {yearback} and {last_date}")
    # Perform a query to retrieve the data and precipitation scores
    sel = [Measurement.date,Measurement.prcp]
    data = session.query(*sel).filter(Measurement.date >= yearback).all()

    # Return the JSON representation of your dictionary.
    # Create a dictionary from the row data and append to a list of all_passengers
    precipitation_data = []
    for row in data:
        precipitation_dict = {}
        precipitation_dict["date"] = row.date
        precipitation_dict["prcp"] = row.prcp
        precipitation_data.append(precipitation_dict)
    
    prcp_data = sorted(precipitation_data, key=itemgetter('date')) 

    return jsonify(prcp_data)

@app.route("/api/v1.0/stations")
def stations():
    #Return a JSON list of stations from the dataset.
    # Design a query to show how many stations are available in this dataset?
    station_count_query = session.query(Station.station,Station.name).all()
    station_list = []
    for row in station_count_query:
        station_dict = {}
        station_dict['station'] = row.station
        station_dict['name'] = row.name
        station_list.append(station_dict)
    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
     # query for the dates and temperature observations from a year from the last data point.
     # Return a JSON list of Temperature Observations (tobs) for the previous year.
    last_data_point_query = session.query(func.max(Measurement.date))
    for record in last_data_point_query:
        last_date = record 
        print(last_date)
    lastdatestring = str(last_date)
    match = re.search('\d{4}-\d{2}-\d{2}', lastdatestring)
    last_date = dt.datetime.strptime(match.group(), '%Y-%m-%d').date()
    yearback = last_date - dt.timedelta(days=365)

    station_measurement_query = session.query(Measurement.station,func.count(Measurement.date)).group_by(Measurement.station).\
                                              order_by(func.count(Measurement.date).desc())
    result = [r.station for r in station_measurement_query]
    mostactivestation = result[0]
    sel_tobs = [Measurement.station,Measurement.date,Measurement.tobs]
    query_tobs = session.query(*sel_tobs).filter(Measurement.date >= yearback).filter(Measurement.station == mostactivestation)
    data_tobs = query_tobs.all()
    tobs_list = []
    for row in data_tobs:
        tobs_dict = {}
        tobs_dict['station'] = row.station
        tobs_dict['date'] = row.date
        tobs_dict['tobs'] = row.tobs
        tobs_list.append(tobs_dict)
    return jsonify(tobs_list)

#Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
@app.route("/api/v1.0/<start>") 
def calc_temps(start):
    start = dt.datetime.strptime(start, "%Y-%m-%d").date()
    #When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date.
    start_date_query = session.query(func.min(Measurement.tobs).label('tmin'), func.avg(Measurement.tobs).label('tavg'), func.max(Measurement.tobs).label('tmax')).\
                        filter(Measurement.date >= start).all()
    st_date_list = []
    for row in start_date_query:
        calc_temp_s = {}
        calc_temp_s['tmin'] = row.tmin
        calc_temp_s['tavg'] = row.tavg
        calc_temp_s['tmax'] = row.tmax
        st_date_list.append(calc_temp_s)
    return jsonify(st_date_list)

@app.route("/api/v1.0/<start>/<end>")
def calc_temps2(start, end):
    start = dt.datetime.strptime(start, "%Y-%m-%d").date()
    end = dt.datetime.strptime(end, "%Y-%m-%d").date()
    st_end_date_query =  session.query(func.min(Measurement.tobs).label('tmin'), func.avg(Measurement.tobs).label('tavg'), func.max(Measurement.tobs).label('tmax')).\
                         filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    #When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.
    st_end_date_list = []
    for row in st_end_date_query:
        calc_temp_s_e = {}
        calc_temp_s_e['tmin'] = row.tmin
        calc_temp_s_e['tavg'] = row.tavg
        calc_temp_s_e['tmax'] = row.tmax
        st_end_date_list.append(calc_temp_s_e)
    return jsonify(st_end_date_list)

if __name__ == "__main__":
    app.run(debug=True)