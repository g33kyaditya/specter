import argparse
import sys
import requests
import time
import json
from typing import List, Dict

from worker import run


# It looks like there is no pagination on this api, but still I'm going to limit=100
# as provided in the doc
BASE_URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?limit=100"


class CoinMarketCap(object):
    """
    Polls API for market cap
    """

    def __init__(self, interval):
        self._interval = interval

    def start(self):
        try:
            while True:
                count, timestamp = self._poll()
                print(f"Fetched {count} number of cryptocurrencies at {timestamp}")
                time.sleep(60 * self._interval)  # Sleep for 60 seconds * minutes

        except Exception as e:
            print(f"An exception {e} occured. Exiting!")
            sys.exit(1)

    def _poll(self):
        r = requests.get(BASE_URL)
        data = json.loads(r.text)
        count = self._push_to_q(data)
        return count, data["status"]["timestamp"]

    def _push_to_q(self, data: Dict):
        count = 0
        for crypto_currency in data["data"]["cryptoCurrencyList"]:
            self._push(crypto_currency)
            count += 1
        return count

    def _push(self, crypto: Dict[str, str]):
        run.delay(crypto)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Call coinmarketapi")
    parser.add_argument(
        "--interval",
        type=int,
        metavar="i",
        help="Interval in mins to poll for updates",
        required=True,
    )

    args = parser.parse_args()
    cmc = CoinMarketCap(args.interval)
    cmc.start()
