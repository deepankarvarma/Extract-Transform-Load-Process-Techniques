import csv
import sqlite3
import time
from multiprocessing import Pool


# Case 1: Take records one by one from the CSV, apply ETL one by one
def case1(file):
    start_time = time.time()
    with open(file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            # ETL process here
            pass
    end_time = time.time()
    return end_time - start_time


# Case 2: Export CSV to a Database file, apply transformations to the database file, and convert this file back to CSV
def case2(file):
    start_time = time.time()
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS csv_data (column1, column2, column3)''')
    with open(file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            c.execute('''INSERT INTO csv_data (column1, column2, column3) VALUES (?, ?, ?)''', row)
    conn.commit()
    c.execute('''SELECT * FROM csv_data''')
    data = c.fetchall()
    # ETL process here on data
    with open('new_' + file, 'w', newline='') as new_csvfile:
        writer = csv.writer(new_csvfile)
        writer.writerows(data)
    end_time = time.time()
    return end_time - start_time


# Case 3: (Extension of Case 2 using multithreading) File Created in Case 2 can be split into various files each having various number of records. Implement etl process using multithreading process with pipelinining concept
def case3(file):
    def etl_process(rows):
        # ETL process here
        pass

    start_time = time.time()
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS csv_data (column1, column2, column3)''')
    with open(file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            c.execute('''INSERT INTO csv_data (column1, column2, column3) VALUES (?, ?, ?)''', row)
    conn.commit()
    c.execute('''SELECT * FROM csv_data''')
    data = c.fetchall()
    num_threads = 4
    rows_per_thread = len(data) // num_threads
    with Pool(num_threads) as p:
        p.map(etl_process, [data[i:i+rows_per_thread] for i in range(0, len(data), rows_per_thread)])
    with open('new_' + file, 'w', newline='') as new_csvfile:
        writer = csv.writer(new_csvfile)
        writer.writerows(data)
    end_time = time.time()
    return end_time - start_time


# Main function to run the three methods on each CSV file and plot the results
if __name__ == '__main__':
    files = ['file1.csv', 'file2.csv', 'file3.csv', 'file4.csv', 'file5.csv', 'file6.csv']
    num_records = [1000, 10000, 20000, 30000, 40000, 50000]
    # Method 1: Case 1 - Take records one by one from the CSV, apply ETL one by one
    case1_times = []
    for file in files:
        time_taken = case1(file)
        case1_times.append(time_taken)
    print("Method 1 - Case 1 execution times:", case1_times)

    # Method 2: Case 2 - Export CSV to a Database file, apply transformations to the database file, and convert this file back to CSV
    case2_times = []
    for file in files:
        time_taken = case2(file)
        case2_times.append(time_taken)
    print("Method 2 - Case 2 execution times:", case2_times)

    # Method 3: Case 3 - (Extension of Case 2 using multithreading) File Created in Case 2 can be split into various files each having various number of records. Implement etl process using multithreading process with pipelinining concept
    case3_times = []
    for file in files:
        time_taken = case3(file)
        case3_times.append(time_taken)
    print("Method 3 - Case 3 execution times:", case3_times)

    # Plotting the execution times for each method and each CSV file
    import matplotlib.pyplot as plt

    plt.plot(num_records, case1_times, label="Method 1 - Case 1")
    plt.plot(num_records, case2_times, label="Method 2 - Case 2")
    plt.plot(num_records, case3_times, label="Method 3 - Case 3")
    plt.xlabel("Number of Records")
    plt.ylabel("Execution Time (Seconds)")
    plt.title("Comparison of Execution Times for Different ELT Methods")
    plt.legend()
    plt.show()

