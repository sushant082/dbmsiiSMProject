import psycopg2
import csv
from psycopg2 import sql

def connect_to_db(dbName):
    return psycopg2.connect(
        dbname=dbName,
        user="postgres",
        password="root",
        host="localhost",
        port=5432
    )

def execute(dbName,cmdTxt):
    with connect_to_db(dbName) as con:
        with con.cursor() as cur:
            cur.execute(cmdTxt)
            res = cur.fetchone()
    return res

def executemany(dbName,cmdTxt):
    with connect_to_db(dbName) as con:
        with con.cursor() as cur:
            cur.execute(cmdTxt)
            res = cur.fetchall()
    return res

def executeNonQuery(dbName,cmdTxt):
    with connect_to_db(dbName) as con:
        with con.cursor() as cur:
            cur.execute(cmdTxt)
            con.commit()

# create database and base tables
def create_database(dbName, table_name):
    con = psycopg2.connect(
        user="postgres",
        password="root",
        host="localhost",
        port=5432
    )
    con.autocommit = True
    with con.cursor() as cur:
        cur.execute(sql.SQL(f"DROP DATABASE IF EXISTS \"{dbName}\""))
        cur.execute(sql.SQL(f"CREATE DATABASE \"{dbName}\""))
        con.commit()

    with connect_to_db(dbName) as con:
        with con.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (ticker TEXT, time INTEGER, open REAL, high REAL, low REAL, close REAL, volume INTEGER)")
            con.commit()

def create_table(dbName, tblName):
    with connect_to_db(dbName) as con:
        with con.cursor() as cur:
            cur.execute(f"CREATE TABLE IF NOT EXISTS {tblName}(stock TEXT, time INTEGER, open REAL, high REAL, low REAL, close REAL, sma21 REAL, rangeratio21 REAL)")
            con.commit()

def create_stock_table(dbName):
    with connect_to_db(dbName) as con:
        con.autocommit = True
        with con.cursor() as cur:
            stockListTxt = "./StockList.txt"
            with open(stockListTxt, newline='') as file:
                reader = csv.reader(file)
                for row in reader:
                    tblName = row[0]  # each row contains the table name
                    cur.execute(f"CREATE TABLE IF NOT EXISTS public.{tblName}(stock TEXT, time INTEGER, open REAL, high REAL, low REAL, close REAL, sma21 REAL, rangeratio21 REAL)")
                    con.commit()
                    print(f"\tCREATED TABLE {tblName}")

def insert_many(dbName, table_name, data):
    with connect_to_db(dbName) as con:
        with con.cursor() as cur:
            sql = (f"INSERT INTO public.{table_name} (stock, time, open, high, low, close, sma21, rangeratio21) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
            cur.executemany(sql, data)
            con.commit()

def insert_many_tickers(dbName, table_name, data):
    with connect_to_db(dbName) as con:
        with con.cursor() as cur:
            sql = (f"INSERT INTO public.{table_name} (ticker) VALUES (%s)")
            cur.executemany(sql, [(ticker,) for ticker in data])
            con.commit()

def insert_csv_data(dbName, table_name, data):
    with connect_to_db(dbName) as con:
        with con.cursor() as cur:
            cur.execute(f"INSERT INTO public.{table_name} VALUES {data}")
            con.commit()

# def delete_data(dbName, user, password, table_name, host='localhost', port='5432'):
#     with connect_to_db(dbName, user, password, host, port) as con:
#         with con.cursor() as cur:
#             cur.execute(f"DELETE FROM {table_name}")
#             con.commit()
