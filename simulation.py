import data_importer
import data_processor
import db_connections
import output_generator
import polygon_api
from datetime import datetime
import time
import threading
import concurrent.futures

RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
RESET = "\033[0m"  # Reset to default color

# Define a worker function for the thread for full data + calculation
def ins_worker(dbName, motherTable):
    for processingTime in range(400, 1959):
        if abs(processingTime) % 100 <= 59:
            start_time = time.time()
            
            print(datetime.now().strftime("\t%Y%m%d-%H:%M:%S") + f"\tSTART: Data Insertion at {processingTime}")
            data_processor.data_insert_full(dbName, motherTable, processingTime)
            print(datetime.now().strftime("\t%Y%m%d-%H:%M:%S") + f"\tFINISH: Data Insertion at {processingTime}")
            
            end_time = time.time()
            print(f"{BLUE}INSERT: Execution time: {end_time - start_time:.4f} seconds{RESET}")


# Define a worker function for the thread for full data + calculation
def tickers_in_minute(dbName, allTickersTable):
    for processingTime in range(1200, 1600):
        if abs(processingTime) % 100 <= 59:
            start_time = time.time()
            
            print(datetime.now().strftime("\t%Y%m%d-%H:%M:%S") + f"\tSTART: Data Insertion at {processingTime}")
            polygon_api.get_minute_ticker(dbName, allTickersTable, processingTime)
            print(datetime.now().strftime("\t%Y%m%d-%H:%M:%S") + f"\tFINISH: Data Insertion at {processingTime}")
            
            end_time = time.time()
            print(f"{BLUE}INSERT: Execution time: {end_time - start_time:.4f} seconds{RESET}")

def load_from_api(dbName, tableName, dateToday):
    start_time = time.time()
    
    print(datetime.now().strftime("%Y%m%d-%H:%M:%S") + "\tSTART: Data loading from API")
    data_importer.load_api_data(dbName, tableName, dateToday)
    print(datetime.now().strftime("%Y%m%d-%H:%M:%S") + "\tFINISH: Data loading from API")

    end_time = time.time()
    print(f"{BLUE}INSERT: Execution time: {end_time - start_time:.4f} seconds{RESET}")

    thread11 = threading.Thread(target=ins_worker, args=(dbName,"mother_Table",))
    thread12 = threading.Thread(target=calc_worker, args=(dbName,))
    thread11.start()
    thread12.start()
    thread11.join()
    thread12.join()


# Define a worker function for the  calculations and file output at each time slot
def calc_worker(dbName):
    smatxt = []
    rrtxt = []
    for processingTime in range(400, 1959):
        if abs(processingTime) % 100 <= 59:
            start_time = time.time()

            fileprefix = datetime.now().strftime("%Y-%m-%d")+'-'+str(processingTime)
            
            print(datetime.now().strftime("\t%Y%m%d-%H:%M:%S") + f"\tSTART: Calculation of SMA21 at {processingTime}")
            sma_res = data_processor.calculate_sma(dbName, processingTime, fileprefix)
            print(datetime.now().strftime("\t%Y%m%d-%H:%M:%S") + f"\tFINISH: Calculation of SMA21 at {processingTime}")

            print(datetime.now().strftime("\t%Y%m%d-%H:%M:%S") + f"\tSTART: Calculation of RR21 at {processingTime}")
            rr_res = data_processor.calculate_range_ratio(dbName, processingTime, fileprefix)
            print(datetime.now().strftime("\t%Y%m%d-%H:%M:%S") + f"\tFINISH: Calculation of RR21 at {processingTime}")

            smatxt.append(sma_res)
            rrtxt.append(rr_res)
            end_time = time.time()
            print(f"{YELLOW}CALC: Execution time: {end_time - start_time:.4f} seconds{RESET}")

    output_generator.write_to_file(f"./sma/{fileprefix}-SMA.txt", smatxt)
    output_generator.write_to_file(f"./rr/{fileprefix}-RangeRatio.txt", rrtxt)

def main():
    # create db
    start_time_all = time.time()
    dateToday = datetime.now().strftime("%Y_%m_%d")
    # dbName = f"2024_11_10-Phase2"
    # 2024_11_10-Phase2
    dbName = f"{dateToday}-Phase2"
    tableName = "stockdata"
    
    db_connections.create_database(dbName, tableName)
    motherTable = "mother_table"
    db_connections.create_table(dbName, motherTable)
    print(datetime.now().strftime("%Y%m%d-%H:%M:%S") + f"\tCREATED DB: {dbName}")

    # Load data
    # print(datetime.now().strftime("%Y%m%d-%H:%M:%S") + "\tSTART: Data loading from CSV")
    # data_importer.load_csv_data(dbName, tableName)
    # print(datetime.now().strftime("%Y%m%d-%H:%M:%S") + "\tFINISH: Data loading from CSV")
    
    # Process data and insert into database
    print(datetime.now().strftime("%Y%m%d-%H:%M:%S") + "\tSTART: Creating Stock tables")
    db_connections.create_stock_table(dbName)
    print(datetime.now().strftime("%Y%m%d-%H:%M:%S") + "\tFINISH: Creating Stock tables")
    
    dateToday = datetime.now().strftime("%Y-%m-%d")
    load_from_api(dbName, tableName, '2024-11-15')

    print(datetime.now().strftime("%Y%m%d-%H:%M:%S") + "\tSTART: Insert in each stock tables")
    data_processor.data_insert_each(dbName, motherTable)
    print(datetime.now().strftime("%Y%m%d-%H:%M:%S") + "\tFINISH: Insert in each stock tables")

    end_time_all = time.time()
    print(f"\n{GREEN}Total Execution Duration: {(end_time_all - start_time_all)/60:.2f} minutes{RESET}")

if __name__ == "__main__":
    main()