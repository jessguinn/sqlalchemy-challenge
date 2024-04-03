# Import the dependencies.
import numpy as np
import sqlalchemy
import datetime as dt 
import pandas as pd
import json
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    """List all available api routes."""
    return (
        f"Welcome to Climate App!<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/surfs-up<br/>"
        f"/api/v1.0/surfs-up/start-date/<start><br/>"
        f"/api/v1.0/surfs-up/start-date/<start>/end-date/<end><br/>"
        f"/api/v1.0/surfs-up/precipitation<br/>"
        f"/api/v1.0/surfs-up/stations<br/>"
        f"/api/v1.0/surfs-up/tobs<br/>"
    )

@app.route("/api/v1.0/surfs-up/precipitation")
def precipitation():
    #Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all measurement dates and precipitaion of the last year"""
    results = session.query(Measurement.date, Measurement.prcp).filter((Measurement.date) >=dt.date(2016, 8, 23)).all()

    session.close()

    # Create a dictionary from the row data
    all_prcp = []
    for date, prcp in results:
        date_dict = {}
        date_dict["date"] = date
        date_dict["prcp"] = prcp
        all_prcp.append(date_dict)

    return jsonify(all_prcp)

@app.route("/api/v1.0/surfs-up/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all stations"""
    results = session.query(Station.station, Station.name).all()

    session.close()


    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)

@app.route("/api/v1.0/surfs-up/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return jsonified data for most active station (USC00519281) only for the last year"""
    active_stations = session.query(Measurement.station, func.count(Measurement.station))\
                             .group_by(Measurement.station)\
                             .order_by(func.count(Measurement.station).desc())\
                             .all()
    session.close()

    # Query most active station
    most_active_station = active_stations[0][0]

    # Query tobs for most active station in the last year
    last_date = session.query(func.max(Measurement.date)).scalar()
    one_year_ago = dt.datetime.strptime(last_date, '%Y-%m-%d') - dt.timedelta(days=365)

    # Query tobs results for most active station
    results = session.query(Measurement.date, Measurement.tobs)\
                     .filter(Measurement.station == most_active_station)\
                     .filter(Measurement.date >= one_year_ago)\
                     .all()

    session.close()

    # Create list from the row data
    tobs_most_active = [{"date": date, "tobs": tobs} for date, tobs in results]

    return jsonify({"most_active_station": most_active_station, "tobs": tobs_most_active})

@app.route("/api/v1.0/surfs-up/start-date/<start>")
def start_date(start):
    """Fetch temperature data grouped by date from the start date to the end of the dataset."""
    # Canonicalize the start date
    canonicalized = start.strip()

    # Query for temperature data grouped by date from the start date onwards
    tobs_data = session.query(Measurement.date, func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs))\
                      .filter(Measurement.date >= canonicalized)\
                      .group_by(Measurement.date)\
                      .all()

    # If data found, return it
    if tobs_data:
        results = [{"date": date, "min_temperature": min_temp, "max_temperature": max_temp, "avg_temperature": avg_temp} for date, min_temp, max_temp, avg_temp in tobs_data]
        return jsonify({"start_date": canonicalized, "temperature_data": results})

    # If no data found, return 404 error
    return jsonify({"error": f"No temperature data found from {canonicalized} onwards."}), 404

@app.route("/api/v1.0/surfs-up/start-date/<start>/end-date/<end>")
def start_and_end_date(start, end):
    """Fetch temperature data grouped by date from the start date to the end of the dataset."""
    # Canonicalize the start date
    canonicalized = start.strip()
    canonicalized_end = end.strip()

    # Query for temperature data grouped by date from the start date onwards
    tobs_data = session.query(Measurement.date, func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs))\
                      .filter(Measurement.date >= canonicalized)\
                      .filter(Measurement.date <= canonicalized_end)\
                      .group_by(Measurement.date)\
                      .all()
    
    # If data found, return it
    if tobs_data:
        results = [{"date": date, "min_temperature": min_temp, "max_temperature": max_temp, "avg_temperature": avg_temp} for date, min_temp, max_temp, avg_temp in tobs_data]
        return jsonify({"start_date": canonicalized, "end_date": canonicalized_end, "temperature_data": results})

if __name__ == "__main__":
    app.run(debug=True)
