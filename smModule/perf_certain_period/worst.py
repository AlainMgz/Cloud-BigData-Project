from pyspark.sql import SparkSession
import os
import sys


spark = SparkSession.builder.appName("worst performer").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
os.system('clear')

option = sys.argv[1]

def worst_all_time():
    loading_messages = ["Loading   ", "Loading.  ", "Loading.. ", "Loading..."]

    # Path to the folder containing CSV files
    folder_path = "../../stock_market_data/nasdaq/csv/"
    files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    # Get the count of files
    file_count = len(files)

    df = spark.read.csv("../../stock_market_data/nasdaq/csv/AAL.csv", header=True, inferSchema=True)
    first_open = df.first()["Open"]
    last_adj_close = df.select("Adjusted Close").tail(1)[0][0]

    worst_perf = last_adj_close - first_open
    worst_perf_stock = "AAL"

    i = 0
    # Iterate through all CSV files in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            input_file_path = os.path.join(folder_path, file_name)

            # Read the CSV file into a DataFrame
            df = spark.read.csv(input_file_path, header=True, inferSchema=True)

            # Extract relevant values
            first_open = df.first()["Open"]
            last_adj_close = df.select("Adjusted Close").tail(1)[0][0]
            if isinstance(first_open, float) and isinstance(last_adj_close, float):
                all_time_perf = last_adj_close - first_open
                if all_time_perf < worst_perf:
                    worst_perf = all_time_perf
                    worst_perf_stock = file_name.strip(".csv")
            print(f"\r\033[KAnalysing files [{i}/{file_count}]", flush=True, end='')
            i += 1

    print(f"\r\033[KThe stock {worst_perf_stock} has the worst all time performance: {worst_perf}")


if option == "-at":
    worst_all_time()