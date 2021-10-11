
import requests

import json

try:
    with open("data/keys/etherscan.json", "r") as json_file:
        data = json.load(json_file)
        API_KEY = data["key"]
except:
    API_KEY = ""

def get_gas():
    url = "https://api.etherscan.io/api"
    querystring = {"module" : "proxy",
                   "action" : "eth_gasPrice",
                   "apikey" : API_KEY}
    response = requests.request("GET", url, params=querystring)
    data = response.json()
    gas = data["result"]
    gas = int((gas, 0)) / 10**9
    return gas

def get_blockNumber():
    url = "https://api.etherscan.io/api"
    querystring = {"module" : "proxy",
                   "action" : "eth_blockNumber",
                   "apikey" : API_KEY}
    data = response.json()
    blockNumber = data["result"]
    blockNumber = int((blockNumber, 0))
    return blockNumber

def get_transaction_erc721(account_address : str, start_block : int = 0):
    url = "https://api.etherscan.io/api"
    querystring = {"module" : "account",
                   "action" : "tokennfttx",
                   "address" : account_address,
                   "page" : 1,
                   "offset" : 100,
                   "startblock" : 100,
                   "sort" : "desc",
                   "apikey" : API_KEY}
    response = requests.request("GET", url, params=querystring)
    data = response.json()
    events = data["results"]
    res = []
    for event in events:
        new = {key : event[key] for key in ["timeStamp", "contractAddress", "tokenID",
                                            "tokenName", "to", "from"]}
        res.append(new)
    return res
