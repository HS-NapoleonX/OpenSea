
import os
import argparse

import datetime
import time
import requests

import numpy as np
import pandas as pd

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

def request(contract_address, offset):
    url = "https://api.opensea.io/api/v1/assets"
    querystring = {"asset_contract_address": contract_address,
                 "offset" : str(50*offset),
                 "limit": "50",
                 "order_direction" : "asc"}
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers, params=querystring)
    data = response.json()
    return data

def process_asset(asset, Asset):
    token_id = int(asset["token_id"])
    traits = "/".join(["{0}:{1}:{2}".format(trait["trait_type"], trait["value"], trait["trait_count"]) for trait in asset["traits"]])
    ev = Asset(token_id=token_id,
               traits=traits,
               twitter=not(asset["collection"]['twitter_username'] is None),
               telegram=not(asset["collection"]['telegram_url'] is None),
               instagram=not(asset["collection"]['instagram_username'] is None),
               wiki=not(asset["collection"]['wiki_url'] is None),
               medium=not(asset["collection"]['medium_username'] is None))
    return ev

def add_asset(ev, db):
    try:
        db.session.add(ev)
        db.session.commit()
        return True
    except:
        db.session.remove()
        return False

def collect(contract_address, db, Asset, sleep, patience):
    """
    contract_address (str)
    db : database
    Asset : model
    sleep : sleep time between each request
    """
    offset = 0
    pat = 0
    while True:
        new_data = 0
        data = request(contract_address, offset)
        if data.get("asset") is None:
            break
        for asset in data["assets"]:
            try:
                ev = process_asset(asset, Asset)
                new_data += (1 if add_asset(ev, db) else 0)
            except:
                continue
        break
        print("     -- added {0} new asset(s)".format(new_data))
        offset += 1
        if pat == patience: break
        if new_data == 0: pat += 1
        time.sleep(sleep)

if __name__ == "__main__" :

    parser = argparse.ArgumentParser()
    parser.add_argument("contract_address", type=str)
    parser.add_argument("--collection", type=str, default=None)
    parser.add_argument("--sleep", type=int, default=3)
    parser.add_argument("--patience", type=int, default=3)

    args = parser.parse_args()

    app = Flask(__name__)
    path_file = 'data/assets/{0}.db'.format(args.contract_address if args.collection is None else args.collection)
    path_database  = 'sqlite:///' + os.path.join(os.getcwd(), path_file)
    app.config['SQLALCHEMY_DATABASE_URI'] = path_database
    db = SQLAlchemy(app)

    class Asset(db.Model):

        token_id = db.Column(db.Integer, primary_key=True)
        traits = db.Column(db.String(300))
        instagram = db.Column(db.Boolean)
        telegram = db.Column(db.Boolean)
        twitter = db.Column(db.Boolean)
        wiki = db.Column(db.Boolean)
        medium = db.Column(db.Boolean)

    db.create_all()

    collect(args.contract_address, db, Asset, args.sleep, args.patience)

    assets = pd.read_sql('SELECT * FROM Asset', path_database, index_col="token_id")
    number_of_assets = len(assets)
    mean_freq_traits, min_freq_traits, max_freq_traits = [], [], []
    for i, row in assets.iterrows():
        tr = row["traits"]
        try:
            freqs = np.array([int(t.split(":")[-1]) for t in tr.split("/")]) / number_of_assets
            mean_freq_traits.append(np.mean(freqs))
            min_freq_traits.append(np.min([f for f in freqs if f > 0] + [0.5]))
            max_freq_traits.append(np.max([f for f in freqs if f < 1] + [0.5]))
        except:
            mean_freq_traits.append(0.5)
            min_freq_traits.append(0.5)
            max_freq_traits.append(0.5)
    assets["mean_freq_traits"] = mean_freq_traits
    assets["min_freq_traits"] = min_freq_traits
    assets["max_freq_traits"] = max_freq_traits
    assets.to_csv(path_file.replace(".db", ".csv"))
