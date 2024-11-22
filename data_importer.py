import db_connections
import csv
import polygon_api
import threading
from concurrent.futures import ThreadPoolExecutor
import time

cmdData=[]

def fetch_minute_record(ticker, fetch_date):
    
    res = polygon_api.detail_per_ticker(ticker, fetch_date)
    if len(res) > 1:
        minute_data = res
        cmdData.append(f"{str(minute_data).replace("[","").replace("]","")},")

def load_csv_data(dbName, tblName):
    csv_file = "./Sample1MinuteData.csv"
    data = []

    with open(csv_file, newline='') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row if there is one
        for row in reader:
            data.append(row)

    values = ', '.join([f"({', '.join(map(repr, row))})" for row in data])
    db_connections.insert_csv_data(dbName, tblName, values)

def load_api_data00(dbName, tblName, processingTime):
    start_time = time.time()

    stockListTxt = polygon_api.get_minute_ticker(processingTime)
    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = []
        for stock in stockListTxt:
            ticker = stock
            futures.append(executor.submit(fetch_minute_record, ticker, processingTime))
        for future in futures:
            future.result()

    cmdTxt=f"INSERT into {tblName} VALUES"
    cmdTxt = cmdTxt+str(cmdData)[3:-2].replace("\", \"","")
    
    end_time = time.time()
    print(f"API request time: {end_time - start_time:.4f}")

    db_connections.executeNonQuery(dbName,cmdTxt)

def load_api_data(dbName, tblName, fetch_date):
    start_time = time.time()
    stockListTxt = "./StockList.txt"
    
    with open(stockListTxt, newline='') as file:
        reader = csv.reader(file)
        with ThreadPoolExecutor(max_workers=100) as executor:
            futures = []
            for row in reader:
                ticker = row[0]
                # print(ticker)
                futures.append(executor.submit(fetch_minute_record, ticker, fetch_date))
            for future in futures:
                future.result()
    cmdTxt=f"INSERT into {tblName} VALUES ("
    cmdTxt = cmdTxt+str(cmdData)[3:-3].replace("\", \"","")
    db_connections.executeNonQuery(dbName,cmdTxt)
    end_time = time.time()
    print(f"API request time: {end_time - start_time:.4f}")

# if __name__ == "__main__":
#     for proTie in range(1500, 1501):
#         load_api_data("2024_11_10-Phase2", "stockdata", proTie)