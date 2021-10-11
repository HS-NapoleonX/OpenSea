
import os

import datetime

import pandas as pd
import numpy as np

contract2name = {
"0x1CB1A5e65610AEFF2551A50f76a87a7d3fB649C6" : "Cryptoadz (TOADZ)",
"0x4f89Cd0CAE1e54D98db6a80150a824a533502EEa" : "Groupies (GROUPIE)",
"0xFF9C1b15B16263C61d017ee9F65C50e4AE0113D7" : "Loot (LOOT)",
"0x8943c7bac1914c9a7aba750bf2b6b09fd21037e0" : "Lazy Lions (LION)",
"0x57a204aa1042f6e66dd7730813f4024114d74f37" : "CyberKongz (KONGZ)",
"0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb" : "CRYPTOPUNKS (C)",
"0x7Bd29408f11D2bFC23c34f18275bBf23bB716Bc7" : "Meebits (:))",
"0xbad6186E92002E312078b5a1dAfd5ddf63d3f731" : "Anonymice (MICE)",
"0xF4ee95274741437636e748DdAc70818B4ED7d043" : "The Doge Pound (DOGGY)",
"0x60e4d786628fea6478f785a6d7e704777c86a7c6" : "MutantApeYachtClub (MAYC)",
"0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d" : "BoredApeYachtClub (BAYC)",
"0x1a92f7381b9f03921564a437210bb9396471050c" : "Cool Cats (COOL)"
}

allowed_payement_tokens = {"ETH" : 10**18, "WETH" : 10**18, "USDC" : 10**6, "DAI" : 10**18}

if __name__  == "__main__":

    prices = pd.read_csv("data/prices/eth.csv", index_col=0)
    prices.index = prices.time
    prices.index = pd.Index([datetime.datetime.fromtimestamp(t) for t in prices.index])
    prices = prices.loc[prices.index.hour == 0, "open"]
    prices.index = prices.index.date

    contracts = [f.split(".")[0] for f in os.listdir("data/transactions") if f.split(".")[-1] == "db"]
    contracts = [f for f in contracts if os.path.exists(os.path.join("data/assets", f + ".csv"))]

    transactions = pd.DataFrame(columns=["datetime", "date", "contract", "collection", "token_id",
                                         "instagram", "telegram", "twitter", "wiki", "medium",
                                         "mean_freq_traits", "min_freq_traits", "max_freq_traits",
                                         "price_USD", "price_ETH", "buyer_address", "seller_address"])

    for contract in contracts:

        path_transactions = "sqlite:///" + os.path.join(os.getcwd(), "data/transactions/{0}.db".format(contract))
        events = pd.read_sql("SELECT * FROM Event", path_transactions)
        events["date"] = [datetime.datetime.fromtimestamp(d).date() for d in events["timestamp"]]
        events["datetime"] = [datetime.datetime.fromtimestamp(d) for d in events["timestamp"]]
        index = pd.Index([])
        for token in allowed_payement_tokens.keys():
            index = index.union(events.loc[events.payement_token == token].index)
        events = events.loc[index]

        decimals = [allowed_payement_tokens[pt] for pt in events["payement_token"]]
        events["marginal_price"] = events["price"].astype(float) / events["quantity"].astype(float) / decimals
        events = events[["token_id", "date", "datetime", "payement_token", "marginal_price",
                         "seller_address", "buyer_address"]]
        events["contract"] = contract
        events["collection"] = contract2name.get(contract)

        assets = pd.read_csv("data/assets/{0}.csv".format(contract))
        assets = assets[["instagram", "telegram", "twitter", "wiki", "medium",
                         "mean_freq_traits", "min_freq_traits", "max_freq_traits"]]

        fusion = events.join(assets, on="token_id")
        fusion = fusion.join(prices, on="date")

        price_USD = []
        price_ETH = []

        for i, row in fusion.iterrows():
            if row.payement_token in ["ETH", "WETH"]:
                price_ETH.append(row.marginal_price)
                price_USD.append(row.marginal_price * row.open)
            elif row.payement_token in ["USDC", "DAI"]:
                price_USD.append(row.marginal_price)
                price_ETH.append(row.marginal_price / row.open)

        fusion["price_USD"] = price_USD
        fusion["price_ETH"] = price_ETH

        fusion = fusion[["datetime", "date", "contract", "collection", "token_id", "instagram", "telegram",
                         "twitter", "wiki", "medium", "mean_freq_traits",  "min_freq_traits",
                         "max_freq_traits", "price_USD", "price_ETH", "seller_address", "buyer_address"]]

        transactions = transactions.append(fusion, ignore_index=True)


    transactions.to_csv("data/results/transactions.csv")
