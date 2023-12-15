from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys


#spark = SparkSession.builder.appName("Covid period").getOrCreate()
spark = SparkSession.builder.config('spark.driver.host', '127.0.0.1').getOrCreate()
spark.sparkContext.setLogLevel("OFF")
if os.name == 'nt':
    os.system('cls')
else:
    os.system('clear')



def best_perf():

    date_event = 2020
    folder_path = "../../stock_market_data/nasdaq/csv/"
    folder_path_2 = "../../stock_market_data/nyse/csv/"
    list_of_files = os.listdir(folder_path) + os.listdir(folder_path_2)
    files = [f for f in list_of_files if (os.path.isfile(os.path.join(folder_path, f)) or os.path.isfile(os.path.join(folder_path_2, f)))]
    file_count = len(files)
    
    best_perf = 0
    best_perf_stock = ""
    best_percentage = 0
    i=0

    for file_name in files:
        if file_name.endswith(".csv"):
            if os.path.isfile(os.path.join(folder_path, file_name)):
                input_file_path = os.path.join(folder_path, file_name)
            else:
                input_file_path = os.path.join(folder_path_2, file_name)

            # Read the CSV file into a DataFrame
            df = spark.read.csv(input_file_path, header=True, inferSchema=True)

            # Convert the 'Date' column to a DateType
            df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyyy"))

            # Extract the year from the 'Date' column
            df = df.withColumn("Date", F.date_format("Date", "yyyy"))
            
            df = df.filter(df["Date"] == date_event)

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
                    if percentage_tmp > best_percentage:
                        best_perf = perf_tmp
                        best_percentage = percentage_tmp
                        best_perf_stock = file_name.strip(".csv")
                        best_perf_year = year

            print(f"\r\033[KAnalysing files [{i}/{file_count}]", flush=True, end='')
            i += 1

    print(f"\r\033[KThe stock {best_perf_stock} has the best performance during the covid period: {best_perf} ({best_percentage}%) on {best_perf_year}")

def worst_perf():

    date_event = 2020
    folder_path = "../../stock_market_data/nasdaq/csv/"
    folder_path_2 = "../../stock_market_data/nyse/csv/"
    list_of_files = os.listdir(folder_path) + os.listdir(folder_path_2)
    files = [f for f in list_of_files if (os.path.isfile(os.path.join(folder_path, f)) or os.path.isfile(os.path.join(folder_path_2, f)))]
    file_count = len(files)
    
    worst_perf = 0
    worst_perf_stock = ""
    worst_percentage = 0
    i=0

    for file_name in files:
        if file_name.endswith(".csv"):
            if os.path.isfile(os.path.join(folder_path, file_name)):
                input_file_path = os.path.join(folder_path, file_name)
            else:
                input_file_path = os.path.join(folder_path_2, file_name)

            # Read the CSV file into a DataFrame
            df = spark.read.csv(input_file_path, header=True, inferSchema=True)

            # Convert the 'Date' column to a DateType
            df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyyy"))

            # Extract the year from the 'Date' column
            df = df.withColumn("Date", F.date_format("Date", "yyyy"))
            
            df = df.filter(df["Date"] == date_event)

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

    print(f"\r\033[KThe stock {worst_perf_stock} has the worst performance during the covid period: {worst_perf} ({worst_percentage}%) on {best_perf_year}")


def worst_day():

    date_event = 2020
    worst_perf = 0
    worst_percentage = 0
    worst_perf_day = ""
    input_file = "../../stock_market_data/Dow_Jones.csv" ###like this or just the folder before and if csv?
    # Read the CSV file into a DataFrame
    df = spark.read.csv(input_file, header=True, inferSchema=True)
    df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyyy"))

    # Create the year column from the 'Date' column
    df = df.withColumn("Year", F.year("Date"))

    df = df.filter(df["Year"] == date_event)

    # Extract relevant values
    rdd = df.rdd
    for row in rdd.collect():
        open = row["Open"]
        close = row["Close"]
        day = row["Date"]
        if isinstance(close, float) and isinstance(open, float) and open > 0:
            day_perf_tmp = close - open
            percentage_tmp = (day_perf_tmp / open) * 100
            if percentage_tmp < worst_percentage:
                worst_perf = day_perf_tmp
                worst_percentage = percentage_tmp
                worst_perf_day = day

    print(f"\r\033[KThe {worst_perf_day} was the worst day for the US market (using the Dow Jones indice) during the covid period, it has a lost of: {worst_perf} ({worst_percentage}%)")

def good_worst_day():

    date_event = "2020-03-20"
    folder_path = "../../stock_market_data/nasdaq/csv/"
    folder_path_2 = "../../stock_market_data/nyse/csv/"
    list_of_files = os.listdir(folder_path) + os.listdir(folder_path_2)
    files = [f for f in list_of_files if (os.path.isfile(os.path.join(folder_path, f)) or os.path.isfile(os.path.join(folder_path_2, f)))]
    file_count = len(files)
    
    best_perf = 0
    best_perf_stock = ""
    best_percentage = 0
    best_upgrade = 0
    best_percentage_up = 0
    best_perf_stock2 = ""
    i=0

    for file_name in files:
        if file_name.endswith(".csv"):
            if os.path.isfile(os.path.join(folder_path, file_name)):
                input_file_path = os.path.join(folder_path, file_name)
            else:
                input_file_path = os.path.join(folder_path_2, file_name)

            # Read the CSV file into a DataFrame
            df = spark.read.csv(input_file_path, header=True, inferSchema=True)

            # Convert the 'Date' column to a DateType
            df = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyyy"))
            
            df = df.filter(df["Date"] == date_event)

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
                    upgrade_tmp = adj_close - open
                    percentage_up_tmp = (perf_tmp / open) * 100
                    percentage_tmp = (perf_tmp / open) * 100

                    if percentage_tmp > best_percentage:
                        best_perf = perf_tmp
                        best_percentage = percentage_tmp
                        best_perf_stock = file_name.strip(".csv")
                        best_perf_year = year
                    
                    if upgrade_tmp > best_upgrade:
                        best_upgrade = upgrade_tmp
                        best_percentage_up = percentage_up_tmp
                        best_perf_stock2 = file_name.strip(".csv")
                        best_perf_year = year
                    

            print(f"\r\033[KAnalysing files [{i}/{file_count}]", flush=True, end='')
            i += 1

    print(f"\r\033[KThe stock {best_perf_stock} has the best performance on the{best_perf_year} with: {best_perf} ({best_percentage}%)")
    print(f"\r\033[KBut the stock {best_perf_stock2} has the greatest upgrade on the{best_perf_year} with: {best_upgrade} ({best_percentage_up}%)")

good_worst_day()













