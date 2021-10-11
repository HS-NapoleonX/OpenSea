

import pandas as pd
import numpy as np

from tqdm import tqdm


if __name__ == "__main__" :

    returns = pd.DataFrame(columns=["address", "contract", "collection", "token_id", "diff_USD", "diff_ETH"])

    transactions = pd.read_csv("data/results/transactions.csv", index_col=0)
    transactions["datetime"] = pd.to_datetime(transactions["datetime"])

    contracts = transactions.contract.unique()

    for contract in tqdm(contracts, total=len(contracts)):

        df = transactions.loc[transactions.contract == contract]
        token_ids = df.token_id.unique()

        for token_id in token_ids:

            df_ = df.loc[df.token_id == token_id]

            for i, (_, row) in enumerate(df_.sort_values("datetime").iterrows()):

                if i == 0:
                    last_owner = row.buyer_address
                    last_price_USD = row.price_USD
                    last_price_ETH = row.price_ETH
                else:
                    if row.seller_address == last_owner:
                        returns.loc[len(returns)] = {"address" : last_owner,
                                                     "contract" : row.contract,
                                                     "collection" : row.collection,
                                                     "token_id" : row.token_id,
                                                     "diff_USD" : row.price_USD - last_price_USD,
                                                     "diff_ETH" : row.price_ETH - last_price_ETH}
                    last_owner = row.buyer_address
                    last_price_USD = row.price_USD
                    last_price_ETH = row.price_ETH


    returns.to_csv("data/results/returns.csv")

    profit_per_account = returns.groupby("address").agg({"diff_USD" : ["count", "sum"], "diff_ETH" : ["count", "sum"]})
    print(profit_per_account)
    profit_per_account.to_csv("data/results/account_profit.csv")
