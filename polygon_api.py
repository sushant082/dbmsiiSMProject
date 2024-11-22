import requests
from polygon import RESTClient
from polygon.rest import models
import db_connections
from datetime import datetime, timezone, timedelta
import pytz
import math

sregmi_api_key = "FbMsLde9fLj7eLJindb_vlPY9fDDFsrp"

def api_request(ticker, time_minute):
    # https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/2024-11-15/2024-11-15?adjusted=true&sort=asc&limit=50000&apiKey=FbMsLde9fLj7eLJindb_vlPY9fDDFsrp
    dateToday = datetime.now().strftime("%Y-%m-%d")
    time_minute=str(time_minute)
    formatted_time = time_minute[:-2] + ":" + time_minute[-2:] + ":00"
    fetch_time = f"{dateToday} {formatted_time}"
    dt = datetime.strptime(fetch_time, '%Y-%m-%d %H:%M:%S')
    data_timestamp = int(dt.timestamp() * 1000)  # Convert to milliseconds

    client = RESTClient(sregmi_api_key)
    aggs = client.get_aggs(
        f"{ticker}",
        1,
        "minute",
        f"{data_timestamp}",
        f"{data_timestamp}"
    )

    # print(aggs)
    # input()

    formatted_aggs = ""
    for agg in aggs:
        timestamp_sec = agg.timestamp // 1000  # Convert milliseconds to seconds
        dt = datetime.fromtimestamp(timestamp_sec)
        hour = dt.hour
        minute = dt.minute
        formatted_time = f"{hour:02}{minute:02}"
        open_price = agg.open
        high_price = agg.high
        low_price = agg.low
        close_price = agg.close
        volume = agg.volume
        formatted_aggs = f"'{ticker}', {formatted_time}, {open_price}, {high_price}, {low_price}, {close_price}, {volume}"
    if(formatted_aggs != ""):
        return formatted_aggs


def detail_per_ticker(ticker, fetch_date):
    fetch_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{fetch_date}/{fetch_date}?adjusted=true&sort=asc&limit=50000&apiKey={sregmi_api_key}"
    print(fetch_url)
    ticker_detail = requests.get(fetch_url).json()
    formatted_data = []

    if 'results' not in ticker_detail:
        return ""

    for record in ticker_detail['results']:
        # formatted_time = (datetime.fromtimestamp(record['t'] / 1000, tz=timezone.utc)-timedelta(hours=5)).strftime('%H%M')
        formatted_time = (datetime.fromtimestamp(record['t'] / 1000, tz=timezone.utc)).strftime('%H%M')
        formatted_data.append((
            ticker_detail['ticker'],
            int(formatted_time),
            record['o'],
            record['h'],
            record['l'],
            record['c'],
            record['v']
        ))
    # print(formatted_data)
    # input()
    return formatted_data

# if __name__ == "__main__":
#     api_request("GOOG", "400")
#     # get_minute_ticker("abc","abc","abc")