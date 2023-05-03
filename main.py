import csv
import sqlite3
import time
import os
from multiprocessing import Pool

if not os.path.exists('new_files'):
    os.makedirs('new_files')

# Case 1: Take records one by one from the CSV, apply ETL one by one
def case1(file):
    start_time = time.time()
    with open(file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            transformed_row = etl_process(row)
            # ETL process here
            print(transformed_row)
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
            c.execute('''INSERT INTO csv_data (column1, column2, column3) VALUES (?, ?, ?)''', (row[0], row[1], row[2]))

    conn.commit()
    c.execute('''SELECT * FROM csv_data''')
    data = c.fetchall()
    transformed_data = etl_process(data)
    with open('new_' + file, 'w', newline='') as new_csvfile:
        writer = csv.writer(new_csvfile)
        writer.writerows(transformed_data)
    end_time = time.time()
    return end_time - start_time


# Case 3: (Extension of Case 2 using multithreading) File Created in Case 2 can be split into various files each having various number of records. Implement etl process using multithreading process with pipelinining concept

def etl_process(row):
    id, firstname, email, email2, profession = row[:5]
    column1 = id
    column2 = firstname[0].upper()
    column3 = f"{email}; {email2}"
    transformed_row = (column1, column2, column3, profession)
    return transformed_row


def process_chunk(data_chunk):
    transformed_data = []
    for row in data_chunk:
        transformed_row = etl_process(row)
        transformed_data.append(transformed_row)
    return transformed_data

def case3(file):
    start_time = time.time()
    conn = sqlite3.connect('database.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS csv_data (column1, column2, column3)''')
    with open(file, 'r') as csvfile:
        reader = csv.reader(csvfile)
        data = [row for row in reader]
    
    num_threads = 4
    rows_per_thread = len(data) // num_threads

    with Pool(num_threads) as p:
        results = []
        for i in range(0, len(data), rows_per_thread):
            chunk = data[i:i+rows_per_thread]
            results.append(p.apply_async(process_chunk, args=(chunk,)))
        p.close()
        p.join()

    transformed_data = []
    for r in results:
        transformed_data += r.get()

    with open('new_' + file, 'w', newline='') as new_csvfile:
        writer = csv.writer(new_csvfile)
        writer.writerows(transformed_data)

    end_time = time.time()
    return end_time - start_time


# Main function to run the three methods on each CSV file and plot the results
if __name__ == '__main__':
    files = ['files/file1.csv', 'files/file2.csv', 'files/file3.csv', 'files/file4.csv', 'files/file5.csv', 'files/file6.csv']
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

    plt.plot(num_records, case1_times, label="Case 1")
    plt.plot(num_records, case2_times, label="Case 2")
    plt.plot(num_records, case3_times, label="Case 3")
    plt.xlabel("Number of Records")
    plt.ylabel("Execution Time (Seconds)")
    plt.title("Comparison of Execution Times for Different ELT Methods")
    plt.legend()
    plt.show()

