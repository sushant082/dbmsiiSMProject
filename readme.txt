REQUIREMENTS
    1. Python   python --version
    2. PostgreSQL installed     https://www.postgresql.org/download/
    3. psycopg2 => The PostgreSQL database adapter for the Python
        python -m pip install psycopg2
    4. pg_credentials:
        pg_credentials = {
            "hostname": "your-hostname",
            "port": "your-port",
            "database": "your-database",
            "user": "your-username",
            "password": "your-password"
        }
    5. Files:
        ./Sample1MinuteData.csv
        ./StockList.txt
    6. Directories:
        ./rr/
        ./sma/
    7. Python libraries
        csv
        datetime
        time
        threading

USAGE
    python ./simulation.py

RECOMMENDATION
    Use of venv to localize the development and execution
    To avoid the need to install Python packages globally.
        python -m venv stocksenv
        .\stocksenv\Scripts\activate

https://chatgpt.com/share/66e8c398-dd98-8011-a5dc-bd18db1a9e5e



user postgres
pwd root

resources:
https://www.youtube.com/watch?v=KPA0Bo3VVgw
https://www.reddit.com/r/PolygonIO/comments/yxl6vf/how_to_get_all_tickers_historically/




GenAi questions:
https://chatgpt.com/share/66e8c398-dd98-8011-a5dc-bd18db1a9e5e

Prompts:
1. thread in loop
2. date to timestamp
3. bulk insert csv in postgres
4. from list
5. tickers = [item['ticker'] for item in ticker_detail['tickers']]
    can i put a filter condition of time = now 
6. if the filter is another json cchild node
7. check if two timestamp are same minute or not
8. tickers = [item['ticker'] for item in ticker_detail['tickers'] if datetime.strptime(str(item['lastTrade']['t'], "%Y-%m-%d %H:%M:%S")).minute==data_timestamp.minute]
    TypeError: decoding to str: need a bytes-like object, int found
9. OSError: [Errno 22] Invalid argument
10. round off timestamp to floor value of minute
11. tickers = [item['ticker'] for item in ticker_detail['tickers'] if item['lastTrade']['t'].replace(second=0, microsecond=0) == data_timestamp]
                                                                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'int' object has no attribute 'replace'
12. tickers = [item['ticker'] for item in ticker_detail['tickers'] if item['lastTrade']['t'] > 0 and datetime.fromtimestamp(item['lastTrade']['t']).replace(second=0, microsecond=0) == data_timestamp]
                                                                                                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
OSError: [Errno 22] Invalid argument
13. nanosecond accuracy SIP Unix Timestamp.
14. nanosec 1731546001823824896
    milisec 1730397600000
15. check for the round down minute in nanosecond against milisecond timestamp
16. tickers = [item['ticker'] for item in ticker_detail['tickers'] if item['updated'] > 0 and datetime.fromtimestamp(item['updated'] // 1_000_000_000) + timedelta(microseconds=item['updated'] % 1_000_000_000/ 1000).replace(second=0, microsecond=0).replace(second=0, microsecond=0) == data_timestamp.replace(second=0, microsecond=0)]
                                                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'datetime.timedelta' object has no attribute 'replace'
17. tickers = [item['ticker'] for item in ticker_detail['tickers'] if item['updated'] > 0 and (datetime.fromtimestamp(item['updated'] // 1_000_000_000) + timedelta(microseconds=item['updated'] % 1_000_000_000/ 1000)).replace(second=0, microsecond=0).replace(second=0, microsecond=0) == data_timestamp.replace(second=0, microsecond=0)]
                                                                                            ^^^^^^^^^^^^^^^^^^^^^^
AttributeError: 'int' object has no attribute 'replace'
18. fetch_time = f"2024-10-31 {formatted_time}"
    dt = datetime.strptime(fetch_time, '%Y-%m-%d %H:%M:%S')
    data_timestamp = int(dt.timestamp() * 1000)
19. nanosecond to second