from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import os
import sys
import shutil
import re
from datetime import datetime

spark = SparkSession.builder.appName("stock spec").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
if os.name == 'nt':
    os.system('cls')
else:
    os.system('clear')

stock_path = sys.argv[1]
regex_pattern = r'/([^/]+)/[^/]+/([^/]+\.csv)$'
match = re.search(regex_pattern, stock_path)
market_name = match.group(1)
stock_name = match.group(2)

def stock_spec_stats():
    i = 0
    animation_chars = ['/', '-', '\\', '|']
    # Read the CSV file into a DataFrame
    df = spark.read.csv(stock_path, header=True, inferSchema=True)

    df_date = df.withColumn("Date", F.to_date(F.col("Date"), "dd-MM-yyyy"))

    df_month = df_date.withColumn("Date", F.date_format("Date", "MM-yyyy"))
    df_year = df_date.withColumn("Date", F.date_format("Date", "yyyy"))

    unused_cols = ["Low","Open","Volume","High","Close"]
    df_year_plot = df_date.drop(*unused_cols)
    rdd_year_plot = df_year_plot.rdd
    last_year = rdd_year_plot.collect()[-1][0].year
    rdd_year_plot = rdd_year_plot.filter(lambda x: x[0].year == last_year)
    
    year_data = rdd_year_plot.collect()
    x_values, y_values = zip(*year_data)
    plt.plot(x_values, y_values, marker='o', linestyle='-')
    plt.xlabel('Date')
    plt.ylabel('Value (USD)')
    plt.title(f"Stock Value ${stock_name.strip('.csv')} - {market_name.capitalize()}")
    plt.grid(True)
    plt.show()

    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')


    df_month = df_month.groupBy("Date").agg(
        F.first("Open").alias("Open"),
        F.last("Adjusted Close").alias("Adjusted Close")
    )
    df_year = df_year.groupBy("Date").agg(
        F.first("Open").alias("Open"),
        F.last("Adjusted Close").alias("Adjusted Close")
    )

    all_time_low = df.first()["Low"]
    all_time_low_day = df.first()["Date"]
    all_time_high = df.first()["High"]
    all_time_high_day = df.first()["Date"]
    best_perf_d = 0
    best_percentage_d = 0
    best_perf_day = ""

    rdd = df.rdd
    for row in rdd.collect():
        high = row["High"]
        low = row["Low"]
        open = row["Open"]
        adj_close = row["Adjusted Close"]
        day = row["Date"]
        adj_close = row["Adjusted Close"]
        if isinstance(high, float) and isinstance(low, float) and isinstance(open, float) and isinstance(adj_close, float) and open != 0:
            if low < all_time_low:
                all_time_low = low
                all_time_low_day = day
            if high > all_time_high:
                all_time_high = high
                all_time_high_day = day
            perf_tmp = adj_close - open
            percentage_tmp = (perf_tmp / open) * 100
            if percentage_tmp > best_percentage_d:
                best_perf_d = perf_tmp
                best_percentage_d = percentage_tmp
                best_perf_day = day

        print(f"\r\033[KLoading... {animation_chars[i]}", flush=True, end='')
        if i == 3:
            i = 0
        else:
            i += 1
    
    last_value = adj_close

    best_perf_m = 0
    best_percentage_m = 0
    best_perf_month = ""

    rdd_month = df_month.rdd
    for row in rdd_month.collect():
        open = row["Open"]
        adj_close = row["Adjusted Close"]
        month = row["Date"]
        if isinstance(adj_close, float) and isinstance(open, float) and open > 0:
            perf_tmp = adj_close - open
            percentage_tmp = (perf_tmp / open) * 100
            if percentage_tmp > best_percentage_m:
                best_perf_m = perf_tmp
                best_percentage_m = percentage_tmp
                best_perf_month = month

        print(f"\r\033[KLoading... {animation_chars[i]}", flush=True, end='')
        if i == 3:
            i = 0
        else:
            i += 1

    best_perf_y = 0
    best_percentage_y = 0
    best_perf_year = ""

    rdd_year = df_year.rdd
    for row in rdd_year.collect():
        open = row["Open"]
        adj_close = row["Adjusted Close"]
        year = row["Date"]
        if isinstance(adj_close, float) and isinstance(open, float) and open > 0:
            perf_tmp = adj_close - open
            percentage_tmp = (perf_tmp / open) * 100
            if percentage_tmp > best_percentage_y:
                best_perf_y = perf_tmp
                best_percentage_y = percentage_tmp
                best_perf_year = year

        print(f"\r\033[KLoading... {animation_chars[i]}", flush=True, end='')
        if i == 3:
            i = 0
        else:
            i += 1
        
    atl_str = f"All Time Low: {round(all_time_low, 4)} on {all_time_low_day}"
    ath_str = f"All Time High: {round(all_time_high, 4)} on {all_time_high_day}"
    length = max(len(ath_str), len(atl_str))
    c = "─"
    s = " "
    a = "*"
    print("\r\033[K" + a * (10) + s + "$" + stock_name.strip('.csv') + " - " + market_name.capitalize() + s + a * (shutil.get_terminal_size()[0] - (16 + len(stock_name.strip('.csv')) + len(market_name))))
    print(f"╭──{c * length}──╮")
    print(f"│  {atl_str: <{length}}  │")
    print(f"│  {ath_str: <{length}}  │")
    print(f"╰──{c * length}──╯")
    print(f"* Last recorded stock value: {round(last_value, 4)} on {day}\n")
    print(f"* Best performance in a day: +{round(best_perf_d, 4)} ({round(best_percentage_d, 4)}%) on {best_perf_day}")
    print(f"* Best performance in a month: +{round(best_perf_m, 4)} ({round(best_percentage_m, 4)}%) on {best_perf_month}")
    print(f"* Best performance in a year: +{round(best_perf_y, 4)} ({round(best_percentage_y, 4)}%) in {best_perf_year}\n")


stock_spec_stats()