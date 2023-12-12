from pyspark.sql import SparkSession
import os
import sys


spark = SparkSession.builder.appName("worst performer").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
if os.name == 'nt':
    os.system('cls')
else:
    os.system('clear')

market_choice = sys.argv[1]
time_frame = sys.argv[2]

def worst_all_time(market):

    if market == "both":
        folder_path = "stock_market_data/nasdaq/csv/"
        folder_path_2 = "stock_market_data/nyse/csv/"
        list_of_files = os.listdir(folder_path) + os.listdir(folder_path_2)
        files = [f for f in list_of_files if (os.path.isfile(os.path.join(folder_path, f)) or os.path.isfile(os.path.join(folder_path_2, f)))]
        file_count = len(files)
    else:
        folder_path = f"stock_market_data/{market}/csv/"
        list_of_files = os.listdir(folder_path)
        files = [f for f in list_of_files if os.path.isfile(os.path.join(folder_path, f))]
        file_count = len(files)

    df = spark.read.csv(os.path.join(folder_path, files[0]), header=True, inferSchema=True)
    first_open = df.first()["Open"]
    last_adj_close = df.select("Adjusted Close").tail(1)[0][0]

    worst_perf = last_adj_close - first_open
    worst_perf_stock = files[0].strip(".csv")

    i = 0
    # Iterate through all CSV files in the folder
    for file_name in list_of_files:
        if file_name.endswith(".csv"):
            if market == "both":
                if os.path.isfile(os.path.join(folder_path, file_name)):
                    input_file_path = os.path.join(folder_path, file_name)
                else:
                    input_file_path = os.path.join(folder_path_2, file_name)
            else:
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
                    worst_perf_opening = first_open
                    worst_perf_closing = last_adj_close
                    worst_perf_stock = file_name.strip(".csv")
            print(f"\r\033[KAnalysing files [{i}/{file_count}]", flush=True, end='')
            i += 1

    percentage_loss = ((worst_perf_closing - worst_perf_opening) / worst_perf_opening) * 100
    print(f"\r\033[KThe stock {worst_perf_stock} has the worst all time performance: {worst_perf} ({percentage_loss}%)")


if market_choice == "-b":
    if time_frame == "-at":
        worst_all_time("both")
elif market_choice == "-nq":
    if time_frame == "-at":
        worst_all_time("nasdaq")
elif market_choice == "-ny":
    if time_frame == "-at":
        worst_all_time("nyse")
