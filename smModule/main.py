import subprocess
import os
import time

def clear_screen():
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')

def print_welcome():
    print("""╔═════════════════════════════════════════════════════════════╗
║           Welcome to the US Stock Market Analysis           ║
║                  Press any key to continue                  ║
╚═════════════════════════════════════════════════════════════╝""")
    input("")

def choose_market():
    print("Please choose which market you want to study:\n[1] Nasdaq | [2] NYSE | [3] Both | [quit] Quit")
    return input("Enter your choice: ")

def choose_statistic():
    print("Choose the statistic you want to look at:")
    print("[1] Best performer | [2] Worst performer | [3] Most stable | [b] Go back | [quit] Quit")
    return input("Enter your choice: ")

def choose_time_frame():
    print("Choose the time frame:")
    print("[1] Day | [2] Month | [3] Year | [4] All Time | [b] Go back | [quit] Quit")
    return input("Enter your choice: ")

def main():
    clear_screen()
    print_welcome()
    while True:
        clear_screen()
        market_choice = choose_market()

        try:
            if market_choice == "quit":
                exit()
            else:
                market_choice = int(market_choice)
        except ValueError:
            print("Invalid input. Please enter an integer.")
            continue

        while True:
            clear_screen()
            stat_choice = choose_statistic()

            try:
                if stat_choice == "quit":
                    exit()
                elif stat_choice == "b":
                    break
                else:
                    stat_choice = int(stat_choice)
            except ValueError:
                print("Invalid input. Please enter an integer.")
                continue

            if stat_choice == 1:
                while True:
                    clear_screen()
                    time_frame = choose_time_frame()

                    try:
                        if time_frame == "quit":
                            exit()
                        elif time_frame == "b":
                            break
                        else:
                            time_frame = int(time_frame)
                    except ValueError:
                        print("Invalid input. Please enter an integer.")
                        continue

                    if time_frame == 1:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "--nasdaq", "--day"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "--nyse", "--day"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "-b", "--day"])

                    elif time_frame == 2:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "--nasdaq", "--month"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "--nyse", "--month"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "-b", "--month"])

                    elif time_frame == 3:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "--nasdaq", "--year"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "--nyse", "--year"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "-b", "--year"])

                    elif time_frame == 4:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "--nasdaq", "--all-time"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "--nyse", "--all-time"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/best.py", "-b", "--all-time"])
                    input("Press any key to continue ")
            elif stat_choice == 2:
                while True:
                    clear_screen()
                    time_frame = choose_time_frame()

                    try:
                        if time_frame == "quit":
                            exit()
                        elif time_frame == "b":
                            break
                        else:
                            time_frame = int(time_frame)
                    except ValueError:
                        print("Invalid input. Please enter an integer.")
                        continue

                    if time_frame == 1:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "--nasdaq", "--day"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "--nyse", "--day"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "-b", "--day"])

                    elif time_frame == 2:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "--nasdaq", "--month"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "--nyse", "--month"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "-b", "--month"])

                    elif time_frame == 3:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "--nasdaq", "--year"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "--nyse", "--year"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "-b", "--year"])

                    elif time_frame == 4:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "--nasdaq", "--all-time"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "--nyse", "--all-time"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/worst.py", "-b", "--all-time"])
                    input("Press any key to continue ")
            elif stat_choice == 3:
                while True:
                    clear_screen()
                    time_frame = choose_time_frame()

                    try:
                        if time_frame == "quit":
                            exit()
                        elif time_frame == "b":
                            break
                        else:
                            time_frame = int(time_frame)
                    except ValueError:
                        print("Invalid input. Please enter an integer.")
                        continue

                    if time_frame == 1:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "--nasdaq", "--day"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "--nyse", "--day"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "-b", "--day"])

                    elif time_frame == 2:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "--nasdaq", "--month"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "--nyse", "--month"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "-b", "--month"])

                    elif time_frame == 3:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "--nasdaq", "--year"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "--nyse", "--year"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "-b", "--year"])

                    elif time_frame == 4:
                        if market_choice == 1:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "--nasdaq", "--all-time"])
                        elif market_choice == 2:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "--nyse", "--all-time"])
                        elif market_choice == 3:
                            subprocess.call(["spark-submit", "smModule/perf_certain_period/stable.py", "-b", "--all-time"])
                    input("Press any key to continue ")
        
if __name__ == "__main__":
    main()
