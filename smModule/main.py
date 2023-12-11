print("Welcome to the US Stock Market analysis sofware. If you want to exit, you can type [quit] at any input in the program.")

while True:

    print("Please choose which market do you want to study : [1] Nasdaq, [2] NYSE, [3] Both:.\n")
    inp = input("Enter your choice: ")

    try:
        if inp == "quit":
            exit()
        else:
            market_choice = int(inp)
    except ValueError:
        print("Invalid input. Please enter an integer.")
        continue

    while True:
        print("""Please choose the statistic you want to look at. If you want to go back, enter [b]:\n
[1] Best performer in a certain period |  [2] Worst performer in a certain period |  [3] Most stable in a certain period |  \n""")
        inp = input("Enter your choice: ")
        
        try:
            if inp == "quit":
                exit()
            elif inp == "b":
                break
            else:
                stat_choice = int(inp)
        except ValueError:
            print("Invalid input. Please enter an integer.")
            continue

        if stat_choice == 1:
            print("Best performer")
        elif stat_choice == 2:
            print("Worst performer")
        elif stat_choice == 3:
            print("Most stable")
    
    