from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("worst performer").getOrCreate()

# Path to the folder containing CSV files
folder_path = "../../stock_market_data/nasdaq/csv/"

# Iterate through all CSV files in the folder
for file_name in os.listdir(folder_path):
    if file_name.endswith(".csv"):
        input_file_path = os.path.join(folder_path, file_name)

        # Read the CSV file into a DataFrame
        df = spark.read.csv(input_file_path, header=True, inferSchema=True)

        # Extract relevant values
        first_open = df.first()["Open"]
        last_adj_close = df.select("Adjusted Close").tail(1)[0][0]
        if first_open == None or last_adj_close == None:
            pass
        else:
            all_time_perf = last_adj_close - first_open

            print(f"For {file_name}, All Time Performance: {all_time_perf}")