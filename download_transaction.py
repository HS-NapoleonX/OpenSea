
import os
import argparse

import datetime
import time
import requests

import pandas as pd

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

def request(contract_address, start, end, offset):
    url = "https://api.opensea.io/api/v1/events"
    querystring = {"asset_contract_address": contract_address,
                   "event_type": "successful",
                   "only_opensea" : "false",
                   "offset" : str(50*offset),
                    "limit": "50",
                    "occurred_before" : str(int(end)),
                    "occurred_after" : str(int(start))}
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.json()
    return data

def process_event(event, Event):
    token_id = int(event["asset"]["token_id"])
    timestamp = datetime.datetime.strptime(event["transaction"]["timestamp"], '%Y-%m-%dT%H:%M:%S')
    timestamp = int(datetime.datetime.timestamp(timestamp))
    last_timestamp = timestamp
    id = str(token_id) + str(timestamp)
    ev = Event(id=id, token_id=token_id, timestamp=timestamp, payement_token=event["payment_token"]["symbol"],
               price=event["total_price"], quantity=event["quantity"], seller_address=event["seller"]["address"],
               buyer_address=event["winner_account"]["address"])
    return ev, last_timestamp

def add_event(ev, db):
    try:
        db.session.add(ev)
        db.session.commit()
        return True
    except:
        db.session.remove()
        return False

def collect(contract_address, start, end, db, Event, sleep):
    """
    contract_address (str)
    end (str) : end timestamp
    start (str) : start timestamp
    db : database
    Event : model
    sleep : sleep time between each request
    """
    print(" -- collecting {0} data from {1} to {2}".format(contract_address,
                                                           datetime.datetime.fromtimestamp(start),
                                                           datetime.datetime.fromtimestamp(end)))
    offset = 0
    while True:
        new_data = 0
        data = request(contract_address, start, end, offset)
        if data.get("asset_events") is None or len(data["asset_events"]) == 0:
            print("     -- all data have been downloaded")
            break
        for event in data["asset_events"]:
            try:
                ev, last_timestamp = process_event(event, Event)
                new_data += (1 if add_event(ev, db) else 0)
            except:
                continue
        if offset < 195:
            offset += 1
        else:
            offset = 0
            end = last_timestamp
        print("     -- added {0} new sample(s)".format(new_data))
        time.sleep(sleep)

if __name__ == "__main__" :

    parser = argparse.ArgumentParser()
    parser.add_argument("contract_address", type=str)
    parser.add_argument("--collection", type=str, default=None)
    parser.add_argument("--end", type=str, default=str(datetime.date.today()))
    parser.add_argument("--start", type=str, default="2021-01-01")
    parser.add_argument("--sleep", type=int, default=3)

    args = parser.parse_args()

    app = Flask(__name__)
    path_file = 'data/transactions/{0}.db'.format(args.contract_address if args.collection is None else args.collection)
    path_database  = 'sqlite:///' + os.path.join(os.getcwd(), path_file)
    app.config['SQLALCHEMY_DATABASE_URI'] = path_database
    db = SQLAlchemy(app)

    class Event(db.Model):
        id = db.Column(db.String(30), primary_key=True)
        token_id = db.Column(db.Integer)
        timestamp = db.Column(db.Integer)
        payement_token = db.Column(db.String(5))
        price = db.Column(db.String(30))
        quantity = db.Column(db.String(3))
        seller_address = db.Column(db.String(50))
        buyer_address = db.Column(db.String(50))

    db.create_all()

    if os.path.exists(path_file):
        events = pd.read_sql('SELECT * FROM Event', path_database)
        if len(events) >= 1:
            end_ts_df = events.timestamp.max()
            start_ts_df = events.timestamp.min()
            print(" -- data from {0} to {1} found".format(datetime.datetime.fromtimestamp(start_ts_df),
                                                         datetime.datetime.fromtimestamp(end_ts_df)))
        else:
            end_ts_df = None
            start_ts_df = None
    else:
        end_ts_df = None
        start_ts_df = None

    start_ts = datetime.datetime.strptime(args.start, "%Y-%m-%d")
    start_ts = datetime.datetime.timestamp(start_ts)
    end_ts = datetime.datetime.strptime(args.end, "%Y-%m-%d")
    end_ts = datetime.datetime.timestamp(end_ts)
    assert start_ts < end_ts

    if start_ts_df is None and end_ts_df is None:
        collect(args.contract_address, start_ts, end_ts, db, Event, args.sleep)
    elif start_ts_df < start_ts and end_ts < end_ts_df:
        pass
    else:
        if end_ts_df < end_ts:
            collect(args.contract_address, end_ts_df, end_ts, db, Event, args.sleep)
        if start_ts < start_ts_df:
            collect(args.contract_address, start_ts, start_ts_df, db, Event, args.sleep)
