# getthenem.py
# get that data from zips into a sql

import zipfile
import os
import sqlalchemy
import configparser
import csv

import time
import progressbar

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.orm import sessionmaker

# Set us up to sql first

config = configparser.ConfigParser()
config.read("config.cfg")

Base = declarative_base()
engine = create_engine(config["database"]["dbstring"])

class PriceAndDemand(Base):
  __tablename__ = 'price_and_demand'
  id = Column(Integer, primary_key=True)
  REGION = Column(String)
  SETTLEMENTDATE = Column(DateTime)
  TOTALDEMAND = Column(Float)
  RRP = Column(Float)
  PERIODTYPE = Column(String)


# wipe it all first!
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

def convert_falsy(field_data):
  if field_data:
    return field_data
  else:
    return None

head_download_folder = "assets"
report_set = "price_and_demand"
report_path = head_download_folder + '/' + report_set

def load_year(state, year):

  for month in range(12):
    load_file(state, year, month + 1)

def load_file(state, year, month):
  
  filename = "PRICE_AND_DEMAND_%s%02d_%s.csv" % (year, month, state)
  file_with_path = os.path.join(report_path, filename)

  print("Importing " + filename)

  columns = []
  objects = []

  with open(file_with_path, newline='\n') as csvfile:

    bar = progressbar.ProgressBar()

    for row in bar(csv.reader(csvfile)):

      if len(row) == 0: # this data can be a bit dirty. 
        continue

      elif row[0] == "REGION": # this is the header row
        # store this as a list of column headers for later indexing. 
        columns = row
        continue

      else: # yay! Data!

        # only try to proceed if we actually have column headers. 
        # maybe the data is no good
        if len(columns) > 0:

          data_mapping = {
            "REGION": convert_falsy(row[columns.index("REGION")]),
            "SETTLEMENTDATE": convert_falsy(row[columns.index("SETTLEMENTDATE")]),
            "TOTALDEMAND": convert_falsy(row[columns.index("TOTALDEMAND")]),
            "RRP": convert_falsy(row[columns.index("RRP")]),
            "PERIODTYPE": convert_falsy(row[columns.index("PERIODTYPE")]),
          }

          obj = PriceAndDemand(**data_mapping)

          # session.add(obj)
          objects.append(obj)
        
        continue

    session.bulk_save_objects(objects)
    session.commit()

for year in range(1998, 2016):
  load_year("VIC1", year)