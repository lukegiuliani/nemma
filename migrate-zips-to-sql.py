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

class Public_Yestbid_dayoffer(Base):
  __tablename__ = 'public_yestbid_dayoffer'
  id = Column(Integer, primary_key=True)
  YESTBID = Column(String)
  BIDDAYOFFER = Column(String)
  FIVE = Column(String)
  SETTLEMENTDATE = Column(DateTime)
  DUID = Column(String)
  BIDTYPE = Column(String)
  BIDSETTLEMENTDATE = Column(DateTime)
  BIDOFFERDATE = Column(DateTime)
  FIRSTDISPATCH = Column(DateTime)
  FIRSTPREDISPATCH = Column(DateTime)
  DAILYENERGYCONSTRAINT = Column(String)
  REBIDEXPLANATION = Column(String)
  PRICEBAND1 = Column(Float)
  PRICEBAND2 = Column(Float)
  PRICEBAND3 = Column(Float)
  PRICEBAND4 = Column(Float)
  PRICEBAND5 = Column(Float)
  PRICEBAND6 = Column(Float)
  PRICEBAND7 = Column(Float)
  PRICEBAND8 = Column(Float)
  PRICEBAND9 = Column(Float)
  PRICEBAND10 = Column(Float)
  MINIMUMLOAD = Column(Integer)
  T1 = Column(Integer)
  T2 = Column(Integer)
  T3 = Column(Integer)
  T4 = Column(Integer)
  NORMALSTATUS = Column(String)
  LASTCHANGED = Column(DateTime)
  BIDVERSIONNO = Column(Integer)
  MR_FACTOR = Column(String)
  ENTRYTYPE = Column(String)

class Public_Yestbid_peroffer(Base):
  __tablename__ = 'public_yestbid_peroffer'
  id = Column(Integer, primary_key=True)
  YESTBID = Column(String)
  BIDPEROFFER = Column(String)
  THREE = Column(String)
  SETTLEMENTDATE = Column(DateTime)
  DUID = Column(String)
  BIDTYPE = Column(String)
  BIDSETTLEMENTDATE = Column(DateTime)
  BIDOFFERDATE = Column(DateTime)
  TRADINGPERIOD = Column(DateTime)
  MAXAVAIL = Column(String)
  FIXEDLOAD = Column(String)
  ROCUP = Column(String)
  ROCDOWN = Column(String)
  ENABLEMENTMIN = Column(Float)
  ENABLEMENTMAX = Column(Float)
  LOWBREAKPOINT = Column(Float)
  HIGHBREAKPOINT = Column(Float)
  BANDAVAIL1 = Column(Float)
  BANDAVAIL2 = Column(Float)
  BANDAVAIL3 = Column(Float)
  BANDAVAIL4 = Column(Float)
  BANDAVAIL5 = Column(Float)
  BANDAVAIL6 = Column(Integer)
  BANDAVAIL7 = Column(Integer)
  BANDAVAIL8 = Column(Integer)
  BANDAVAIL9 = Column(Integer)
  BANDAVAIL10 = Column(Integer)
  PASAAVAILABILITY = Column(String)
  PERIODID = Column(String)
  LASTCHANGED = Column(DateTime)
  BIDVERSIONNO = Column(Integer)
  MR_CAPACITY = Column(String)

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
report_set = "Current/Yesterdays_Bids_Reports"
table_name = "Yesterdays_Bids_Reports"


report_path = head_download_folder + '/' + report_set

for filename in os.listdir(report_path):

  file_with_path = os.path.join(report_path, filename)
  if filename.endswith(".zip"):
    print("Importing " + filename)

    zip_ref = zipfile.ZipFile(file_with_path, 'r')

    # assume only one file per zip. 
    csv_data = zip_ref.read(zip_ref.namelist()[0]).decode()

    columns = []
    objects = []
    current_data_type = None

    bar = progressbar.ProgressBar(redirect_stdout=True)
    for row in bar(csv.reader(csv_data.split("\n")), len(csv_data.split("\n"))):

      if len(row) == 0: # this data can be a bit dirty. 
        continue

      elif row[0] == "C": # this is the header comment line
        continue

      elif row[0] == "I": # this is the header row
        # store this as a list of column headers for later indexing. 
        columns = row

        # either BIDDAYOFFERs and BIDPEROFFERs
        current_data_type = row[2]
        print("Loading %s rows" % current_data_type)

        continue

      elif row[0] == "D": # yay! Data!

        # okay these files actually have two kinds of data in them. BIDDAYOFFERs and BIDPEROFFERs
        if current_data_type == "BIDDAYOFFER":

          data_mapping = {
            "YESTBID": convert_falsy(row[columns.index("YESTBID")]),
            "BIDDAYOFFER": convert_falsy(row[columns.index("BIDDAYOFFER")]),
            "FIVE": convert_falsy(row[columns.index("5")]),
            "SETTLEMENTDATE": convert_falsy(row[columns.index("SETTLEMENTDATE")]),
            "DUID": convert_falsy(row[columns.index("DUID")]),
            "BIDTYPE": convert_falsy(row[columns.index("BIDTYPE")]),
            "BIDSETTLEMENTDATE": convert_falsy(row[columns.index("BIDSETTLEMENTDATE")]),
            "BIDOFFERDATE": convert_falsy(row[columns.index("BIDOFFERDATE")]),
            "FIRSTDISPATCH": convert_falsy(row[columns.index("FIRSTDISPATCH")]),
            "FIRSTPREDISPATCH": convert_falsy(row[columns.index("FIRSTPREDISPATCH")]),
            "DAILYENERGYCONSTRAINT": convert_falsy(row[columns.index("DAILYENERGYCONSTRAINT")]),
            "REBIDEXPLANATION": convert_falsy(row[columns.index("REBIDEXPLANATION")]),
            "PRICEBAND1": convert_falsy(row[columns.index("PRICEBAND1")]),
            "PRICEBAND2": convert_falsy(row[columns.index("PRICEBAND2")]),
            "PRICEBAND3": convert_falsy(row[columns.index("PRICEBAND3")]),
            "PRICEBAND4": convert_falsy(row[columns.index("PRICEBAND4")]),
            "PRICEBAND5": convert_falsy(row[columns.index("PRICEBAND5")]),
            "PRICEBAND6": convert_falsy(row[columns.index("PRICEBAND6")]),
            "PRICEBAND7": convert_falsy(row[columns.index("PRICEBAND7")]),
            "PRICEBAND8": convert_falsy(row[columns.index("PRICEBAND8")]),
            "PRICEBAND9": convert_falsy(row[columns.index("PRICEBAND9")]),
            "PRICEBAND10": convert_falsy(row[columns.index("PRICEBAND10")]),
            "MINIMUMLOAD": convert_falsy(row[columns.index("MINIMUMLOAD")]),
            "T1": convert_falsy(row[columns.index("T1")]),
            "T2": convert_falsy(row[columns.index("T2")]),
            "T3": convert_falsy(row[columns.index("T3")]),
            "T4": convert_falsy(row[columns.index("T4")]),
            "NORMALSTATUS": convert_falsy(row[columns.index("NORMALSTATUS")]),
            "LASTCHANGED": convert_falsy(row[columns.index("LASTCHANGED")]),
            "BIDVERSIONNO": convert_falsy(row[columns.index("BIDVERSIONNO")]),
            "MR_FACTOR": convert_falsy(row[columns.index("MR_FACTOR")]),
            "ENTRYTYPE": convert_falsy(row[columns.index("ENTRYTYPE")]),
          }

          obj = Public_Yestbid_dayoffer(**data_mapping)

        elif current_data_type == "BIDPEROFFER":

          data_mapping = {
            "YESTBID": convert_falsy(row[columns.index("YESTBID")]),
            "BIDPEROFFER": convert_falsy(row[columns.index("BIDPEROFFER")]),
            "THREE": convert_falsy(row[columns.index("3")]),
            "SETTLEMENTDATE": convert_falsy(row[columns.index("SETTLEMENTDATE")]),
            "DUID": convert_falsy(row[columns.index("DUID")]),
            "BIDTYPE": convert_falsy(row[columns.index("BIDTYPE")]),
            "BIDSETTLEMENTDATE": convert_falsy(row[columns.index("BIDSETTLEMENTDATE")]),
            "BIDOFFERDATE": convert_falsy(row[columns.index("BIDOFFERDATE")]),
            "TRADINGPERIOD": convert_falsy(row[columns.index("TRADINGPERIOD")]),
            "MAXAVAIL": convert_falsy(row[columns.index("MAXAVAIL")]),
            "FIXEDLOAD": convert_falsy(row[columns.index("FIXEDLOAD")]),
            "ROCUP": convert_falsy(row[columns.index("ROCUP")]),
            "ROCDOWN": convert_falsy(row[columns.index("ROCDOWN")]),
            "ENABLEMENTMIN": convert_falsy(row[columns.index("ENABLEMENTMIN")]),
            "ENABLEMENTMAX": convert_falsy(row[columns.index("ENABLEMENTMAX")]),
            "LOWBREAKPOINT": convert_falsy(row[columns.index("LOWBREAKPOINT")]),
            "HIGHBREAKPOINT": convert_falsy(row[columns.index("HIGHBREAKPOINT")]),
            "BANDAVAIL1": convert_falsy(row[columns.index("BANDAVAIL1")]),
            "BANDAVAIL2": convert_falsy(row[columns.index("BANDAVAIL2")]),
            "BANDAVAIL3": convert_falsy(row[columns.index("BANDAVAIL3")]),
            "BANDAVAIL4": convert_falsy(row[columns.index("BANDAVAIL4")]),
            "BANDAVAIL5": convert_falsy(row[columns.index("BANDAVAIL5")]),
            "BANDAVAIL6": convert_falsy(row[columns.index("BANDAVAIL6")]),
            "BANDAVAIL7": convert_falsy(row[columns.index("BANDAVAIL7")]),
            "BANDAVAIL8": convert_falsy(row[columns.index("BANDAVAIL8")]),
            "BANDAVAIL9": convert_falsy(row[columns.index("BANDAVAIL9")]),
            "BANDAVAIL10": convert_falsy(row[columns.index("BANDAVAIL10")]),
            "PASAAVAILABILITY": convert_falsy(row[columns.index("PASAAVAILABILITY")]),
            "PERIODID": convert_falsy(row[columns.index("PERIODID")]),
            "LASTCHANGED": convert_falsy(row[columns.index("LASTCHANGED")]),
            "BIDVERSIONNO": convert_falsy(row[columns.index("BIDVERSIONNO")]),
            "MR_CAPACITY": convert_falsy(row[columns.index("MR_CAPACITY")]),
          }

          obj = Public_Yestbid_peroffer(**data_mapping)

      # session.add(obj)
      objects.append(obj)
      continue

    # commit at the file level
    # session.commit()

    print("Committing...")
    session.bulk_save_objects(objects)
    session.commit()
    zip_ref.close()

  else:
    print("Skipping " + filename)


