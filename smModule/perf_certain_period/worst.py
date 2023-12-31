from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

    worst_perf = 0
    worst_perf_stock = ""
    worst_percentage = 0

    i = 0
    # Iterate through all CSV files in the folder
    for file_name in files:
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
            if isinstance(first_open, float) and isinstance(last_adj_close, float) and first_open > 0:
                all_time_perf_tmp = last_adj_close - first_open
                percentage_tmp = (all_time_perf_tmp / first_open) * 100
                if percentage_tmp < worst_percentage:
                    worst_perf = all_time_perf_tmp
                    worst_percentage = percentage_tmp
                    worst_perf_stock = file_name.strip(".csv")
            print(f"\r\033[KAnalysing files [{i}/{file_count}]", flush=True, end='')
            i += 1

    print(f"\r\033[KThe stock {worst_perf_stock} has the worst all time performance: {worst_perf} ({worst_percentage}%)")

def worst_year(market):
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

    worst_perf = 0
    worst_perf_stock = ""
    worst_percentage = 0
    worst_perf_year = ""

    i = 0
    for file_name in files:
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

            # Convert the 'Date' column to a DateType
            df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyyy"))

            # Extract the month from the 'Date' column
            df = df.withColumn("Date", F.date_format("Date", "yyyy"))

            # Aggregate the open and close values for each month
            df = df.groupBy("Date").agg(
                F.first("Open").alias("Open"),
                F.last("Adjusted Close").alias("Adjusted Close")
            )

            # Extract relevant values
            rdd = df.rdd
            for row in rdd.collect():
                open = row["Open"]
                adj_close = row["Adjusted Close"]
                year = row["Date"]
                if isinstance(adj_close, float) and isinstance(open, float) and open > 0:
                    perf_tmp = adj_close - open
                    percentage_tmp = (perf_tmp / open) * 100
                    if percentage_tmp < worst_percentage:
                        worst_perf = perf_tmp
                        worst_percentage = percentage_tmp
                        worst_perf_stock = file_name.strip(".csv")
                        worst_perf_year = year

            print(f"\r\033[KAnalysing files [{i}/{file_count}]", flush=True, end='')
            i += 1

    print(f"\r\033[KThe stock {worst_perf_stock} has the worst yearly performance ever: {worst_perf} ({worst_percentage}%) on {worst_perf_year}")

def worst_month(market):
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

    worst_perf = 0
    worst_perf_stock = ""
    worst_percentage = 0
    worst_perf_month = ""

    i = 0
    for file_name in files:
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

            # Convert the 'Date' column to a DateType
            df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyyy"))

            # Extract the month from the 'Date' column
            df = df.withColumn("Date", F.date_format("Date", "MM-yyyy"))

            # Aggregate the open and close values for each month
            df = df.groupBy("Date").agg(
                F.first("Open").alias("Open"),
                F.last("Adjusted Close").alias("Adjusted Close")
            )

            # Extract relevant values
            rdd = df.rdd
            for row in rdd.collect():
                open = row["Open"]
                adj_close = row["Adjusted Close"]
                month = row["Date"]
                if isinstance(adj_close, float) and isinstance(open, float) and open > 0:
                    perf_tmp = adj_close - open
                    percentage_tmp = (perf_tmp / open) * 100
                    if percentage_tmp < worst_percentage:
                        worst_perf = perf_tmp
                        worst_percentage = percentage_tmp
                        worst_perf_stock = file_name.strip(".csv")
                        worst_perf_month = month

            print(f"\r\033[KAnalysing files [{i}/{file_count}]", flush=True, end='')
            i += 1

    print(f"\r\033[KThe stock {worst_perf_stock} has the worst monthly performance ever: {worst_perf} ({worst_percentage}%) on {worst_perf_month}")

def worst_day(market):
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

    worst_perf = 0
    worst_perf_stock = ""
    worst_percentage = 0
    worst_perf_day = ""

    i = 0
    for file_name in files:
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
            rdd = df.rdd
            for row in rdd.collect():
                open = row["Open"]
                adj_close = row["Adjusted Close"]
                day = row["Date"]
                if isinstance(adj_close, float) and isinstance(open, float) and open > 0:
                    day_perf_tmp = adj_close - open
                    percentage_tmp = (day_perf_tmp / open) * 100
                    if percentage_tmp < worst_percentage:
                        worst_perf = day_perf_tmp
                        worst_percentage = percentage_tmp
                        worst_perf_stock = file_name.strip(".csv")
                        worst_perf_day = day

            print(f"\r\033[KAnalysing files [{i}/{file_count}]", flush=True, end='')
            i += 1

    print(f"\r\033[KThe stock {worst_perf_stock} has the worst daily performance ever: {worst_perf} ({worst_percentage}%) on {worst_perf_day}")

if market_choice == "-b":
    if time_frame == "---all-time":
        worst_all_time("both")
    elif time_frame == "--day":
        worst_day("both")
    elif time_frame == "--month":
        worst_month("both")
    elif time_frame == "--year":
        worst_year("both")
elif market_choice == "--nasdaq":
    if time_frame == "--all-time":
        worst_all_time("nasdaq")
    elif time_frame == "--day":
        worst_day("nasdaq")
    elif time_frame == "--month":
        worst_month("nasdaq")
    elif time_frame == "--year":
        worst_year("nasdaq")
elif market_choice == "--nyse":
    if time_frame == "--all-time":
        worst_all_time("nyse")
    elif time_frame == "--day":
        worst_day("nyse")
    elif time_frame == "--month":
        worst_month("nyse")
    elif time_frame == "--year":
        worst_year("nyse")
