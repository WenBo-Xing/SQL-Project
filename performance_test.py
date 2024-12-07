import  os
import random
import datetime
import time
import sqlite3
import tempfile
from tabulate import tabulate

def generate_test_data(num_records, start_time=None):
    
    if start_time is None:
        start_time = datetime.datetime(2024, 1, 1, 1, 0, 0)

    test_data = []
    for i in range(num_records):
       
        timestamp = (start_time + datetime.timedelta(seconds=i)).strftime('%Y-%m-%d %H:%M:%S')
        sensor_id = random.randint(10000, 19999)  
        value = round(random.uniform(30.0, 70.0), 1)  
        location = f"Field_{random.randint(1, 7)}"  
        data_type = random.choice(["Temp", "Light", "Humidity"])  
        test_data.append((timestamp, sensor_id, value, location, data_type))

    return test_data


def create_temp_database():
    
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    conn = sqlite3.connect(temp_db.name)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            timestamp TEXT PRIMARY KEY,
            sensor_id TEXT,
            value REAL,
            location TEXT,
            data_type TEXT
        )
    """)
    conn.commit()
    conn.close()
    return temp_db.name

def performance_test(bpt_class, num_records=1000):
    """
    测试 B+ 树的插入、查询性能，包括多种范围大小的查询测试
    """
    temp_db_path = tempfile.NamedTemporaryFile(delete=False, suffix=".db").name

    try:
       
        bpt = bpt_class(order=20, database_path=temp_db_path, table_name="sensor_data")

        print("\nStart Test...")
        test_data = generate_test_data(num_records)

        start_time = time.perf_counter()
        for record in test_data:
            bpt.insert(*record)
        end_time = time.perf_counter()
        insert_time = end_time - start_time
        print(f"Insert {num_records} Data use time: {insert_time:.2f} s")

       
        range_sizes = [10, 100, 1000,10000, min(num_records, 100000)]  
        range_query_times = []
        for range_size in range_sizes:
            start_key = test_data[0][0]  
            end_index = min(range_size - 1, len(test_data) - 1)  
            end_key = test_data[end_index][0]

            start_time = time.perf_counter()
            results = bpt.range_query(start_key, end_key)
            end_time = time.perf_counter()
            query_time = end_time - start_time
            range_query_times.append((range_size, query_time))
            print(f"Range Query {range_size} records use time: {query_time:.6f} s")

       
        single_key = test_data[len(test_data) // 2][0]
        start_time = time.perf_counter()
        result = bpt.search(single_key)
        end_time = time.perf_counter()
        single_query_time = end_time - start_time
        print(f"Single key query use time: {single_query_time:.6f} s")

        
        performance_summary = [
            ["  <<Option Type>>   ", "time (s)"],
            ["  <<Insert>>        ", f"{insert_time:.2f}"],
            *[  
                [f"  <<Range Query ({size} records)>>", f"{time:.6f}"]
                for size, time in range_query_times
            ],
            ["  <<Single key query>>", f"{single_query_time:.6f}"]
        ]
        print("\nResult ：")
        print(tabulate(performance_summary, headers="firstrow", tablefmt="grid"))

    finally:
        if os.path.exists(temp_db_path):
            os.remove(temp_db_path)
