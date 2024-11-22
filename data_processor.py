import csv
import db_connections
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

RED = "\033[91m"
NEON_BLUE = "\033[94m"
RESET = "\033[0m"

##################################TASK 1: 21 SMA##################################
##################################TASK 1: 21 SMA##################################
##################################TASK 1: 21 SMA##################################

def calculate_sma(dbName, time, fileprefix):
    smatxt = []
    cmdTxt = f"""WITH ranked_data AS (
                    SELECT
                    ticker, time, close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY time DESC) AS rn
                    FROM stockdata
                    WHERE time <= {time}
                ),
                avg_21 AS (
                    SELECT
                    ticker,
                    AVG(close) OVER (PARTITION BY ticker) AS avg_close
                    FROM
                    ranked_data
                    where rn <= 21
                )
                select
                ticker, avg_close as sma
                from avg_21
                group by ticker,avg_close"""
    avgClose = db_connections.executemany(dbName, cmdTxt)
    
    if avgClose and avgClose[0] is not None:
        for row in avgClose:
            ticker = row[0]
            smaValTweOne = row[1]
            smatxt.append(f"{time} {ticker} 21 SMA: {round(smaValTweOne, 2)}\n" if smaValTweOne >= 1 else f"{time} {ticker} 21 SMA: {round(smaValTweOne, 3)}\n")

    # output_generator.write_to_file(f"./sma/{fileprefix}-SMA.txt", smatxt)
    return smatxt

##################################TASK 1: 21 SMA##################################
##################################TASK 1: 21 SMA##################################
##################################TASK 1: 21 SMA##################################


##################################TASK 2: RANGE RATIO##################################
##################################TASK 2: RANGE RATIO##################################
##################################TASK 2: RANGE RATIO##################################

def calculate_range_ratio(dbName, time, fileprefix):
    rrtxt = []
    cmdTxt = f"""WITH activity AS (
                select ticker from stockdata where time = {time}
                ),
                ranked_data AS (
                    SELECT
                    ticker, time, high, low, high - low AS curr_rr,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY time DESC) AS rn
                    FROM stockdata
                    WHERE time <= {time}
                    and ticker in (select ticker from activity)
                ),
                filtered_list AS (
                    SELECT
                    ticker,
                    curr_rr,
                    max(high) OVER (PARTITION BY ticker) AS highest_high,
                    min(low) OVER (PARTITION BY ticker) AS lowest_low,
                    RANK() OVER (PARTITION BY ticker order by time desc) AS rn
                    FROM
                    ranked_data
                    where rn < 21
                )
                select
                ticker, curr_rr, highest_high, lowest_low
                from filtered_list
                where rn = 1"""
    rr_data = db_connections.executemany(dbName, cmdTxt)
    if rr_data and rr_data[0] is not None:
        for row in rr_data:
            ticker = row[0]
            curr_rr = row[1]
            hh_ll = row[2] - row[3]
            rangeRatio = curr_rr
            try:
                rangeRatio = curr_rr/hh_ll
            except ZeroDivisionError as e:
                e # print(f"{RED}\t\t{e}\t{ticker}\t{time}{RESET}")

            rrtxt.append(f"{time} {ticker} Range Ratio: {round(rangeRatio, 2)}\n" if rangeRatio >= 1 else f"{time} {ticker} Range Ratio: {round(rangeRatio, 3)}\n")
    # output_generator.write_to_file(f"./rr/{fileprefix}-RangeRatio.txt", rrtxt)
    return rrtxt

##################################TASK 2: RANGE RATIO##################################
##################################TASK 2: RANGE RATIO##################################
##################################TASK 2: RANGE RATIO##################################


##################################TASK 3: WRITE DATABASE TO DISK##################################
##################################TASK 3: WRITE DATABASE TO DISK##################################
##################################TASK 3: WRITE DATABASE TO DISK##################################

def data_insert_full(dbName, motherTable, time):
    cmdTxt = f"""WITH ranked_data AS (
                    SELECT
                    ticker, time, close,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY time DESC) AS rn
                    FROM stockdata
                    WHERE time <= {time}
                ),
                avg_21 AS (
                    SELECT
                    ticker,
                    AVG(close) OVER (PARTITION BY ticker) AS avg_close
                    FROM
                        ranked_data
                    where rn <= 21
                ),
                sma AS(
                    select
                    ticker, {time} as time, avg_close as sma
                    from avg_21
                    group by ticker,avg_close
                ),
                activity AS (
                    select ticker from stockdata where time = {time}
                ),
                ranked_data_rr AS (
                    SELECT
                    ticker, time, open, close, high, low, high - low AS curr_rr,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY time DESC) AS rn
                    FROM stockdata
                    WHERE time <= {time}
                    and ticker in (select ticker from activity)
                ),
                filtered_list AS (
                    SELECT
                    ticker, time, open, close, high, low,
                    curr_rr,
                    max(high) OVER (PARTITION BY ticker) AS highest_high,
                    min(low) OVER (PARTITION BY ticker) AS lowest_low,
                    RANK() OVER (PARTITION BY ticker order by time desc) AS rn
                    FROM
                        ranked_data_rr
                    where rn < 21
                ),
                rr AS (
                    select
                    ticker, time, open, close, high, low, curr_rr, highest_high, lowest_low
                    from filtered_list
                    where rn = 1
                )
                select 
                rr.ticker, rr.time, rr.open, rr.high, rr.low, rr.close,
                sma.sma as sma21,
                CASE
                when highest_high = lowest_low then curr_rr
                ELSE curr_rr/(highest_high - lowest_low)
                END as RangeRatio21
                from rr
                left join sma on sma.ticker = rr.ticker and sma.time = rr.time"""
    dataToInsert=db_connections.executemany(dbName,cmdTxt)
    if dataToInsert and dataToInsert[0] is not None:
        tblName = motherTable
        db_connections.insert_many(dbName,tblName,dataToInsert)

def tbl_write_worker(dbName,ticker,motherTable):
    print(datetime.now().strftime(f"{NEON_BLUE}%Y%m%d-%H:%M:%S") + f"\tDATA INSERT IN {ticker} TABLE {RESET}")
    cmdTxt = f"""
            INSERT INTO public.{ticker}
            SELECT
            stock, time, open, high, low, close, sma21, rangeratio21
            FROM public.{motherTable} WHERE stock = '{ticker}'"""
    db_connections.executeNonQuery(dbName,cmdTxt)
            
def data_insert_each(dbName, motherTable):
    stockListTxt = "./StockList.txt"
    with open(stockListTxt, newline='') as file:
        reader = csv.reader(file)
        tickers = [row[0].strip() for row in reader]

    # Create a thread pool to process each ticker efficiently
    with ThreadPoolExecutor() as executor:
        futures = []
        for ticker in tickers:
            futures.append(executor.submit(tbl_write_worker, dbName, ticker, motherTable))
        
        # Ensure all threads complete before proceeding
        for future in futures:
            future.result()

##################################TASK 3: WRITE DATABASE TO DISK##################################
##################################TASK 3: WRITE DATABASE TO DISK##################################
##################################TASK 3: WRITE DATABASE TO DISK##################################