import sys
import sqlite3
import time
from BPlus_Tree import BPlusTree
from tabulate import tabulate
from performance_test import performance_test
from performance_test import generate_test_data, performance_test
sys.stdout.reconfigure(encoding='utf-8')




def print_menu():
    menu = [
        [" <<1>> ", "Insert data"],
        [" <<2>> ", "TimesStamp Query:"],
        [" <<3>> ", "Range Query"],
        [" <<4>> ", "Delete Data"],
        [" <<5>> ", "ID Query"],
        [" <<6>> ", "Performance Test"],
        [" <<7>> ", "Displays the first 20 pieces of data in the database"],
        ["-" * 7, "-" * 51],
        [" <<0>> ", "EXIT"],
    ]

    # Title
    print("\n+---------+------------------------------------------------------+")
    print(f"| {'Options':<4} | {'Operation':<52} |")
    print("+---------+------------------------------------------------------+")

    # Table Information
    for row in menu:
        print(f"| {row[0]:<4} | {row[1]:<52} |")
    print("+---------+------------------------------------------------------+")


def display_database_records(database_path, table_name, limit=20):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT timestamp, sensor_id, value, location, data_type
        FROM {table_name}
        ORDER BY timestamp ASC
        LIMIT ?
    """, (limit,))
    records = cursor.fetchall()
    conn.close()

    if records:
        headers = ["Timestamp", "Sensor ID", "Value", "Location", "Data Type"]
        print("\nDatabase records：")
        print(tabulate(records, headers=headers, tablefmt="grid"))
    else:
        print("\nNo Database records！")

def load_data_from_database_to_bptree(bpt, database_path, table_name):
    
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT timestamp, sensor_id, value, location, data_type FROM {table_name}")
    records = cursor.fetchall()
    conn.close()

    for record in records:
        timestamp, sensor_id, value, location, data_type = record
        bpt._insert_non_full(bpt.root, timestamp, {
            'timestamp': timestamp,
            'sensor_id': sensor_id,
            'value': value,
            'location': location,
            'data_type': data_type
        })
    return len(records)

def main():
    
    
    database_path = "/Users/xingwenbo/Desktop/4525_FinalProject/sensor_data.db"
    table_name = "sensor_data"
    
    #Start time
    start_time = time.perf_counter()
    
    bpt = BPlusTree(order=20, database_path=database_path, table_name=table_name)

    # load database to B+ tree
    data_count = load_data_from_database_to_bptree(bpt, database_path, table_name)
    
    #End time
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    
    print(f"\n Loading data done! Total number of data: {data_count},  Run Times: {elapsed_time:.2f} s \n")

    #********************
    #Menu
    #********************
    while True:
        print_menu()
        choice = input("\nInput Options  (1/2/3/4/5/6/7): ")
        
    
        
        if choice == '1':
            timestamp = input("Input Timestamp (Format: YYYY-MM-DD HH:MM:SS): ")
            sensor_id = int(input("Input Sensor ID (5 number): "))  
            value = input("Input Value: ")
            location = input("Input Location: ")
            data_type = input("Input Data type: ")
            try:
                bpt.insert(timestamp, sensor_id, float(value), location, data_type)
                print("Successful insert！")
            except ValueError as e:
                print(f"Fail：{e}")
                
                
                
        elif choice == '2':
            key = input("Input timeStamp (Format: YYYY-MM-DD HH:MM:SS): ")
            result = bpt.search(key)
            if result:
                headers = ["Timestamp", "Sensor ID", "Value", "Location", "Data Type"]
                data = [[key, result['sensor_id'], result['value'], result['location'], result['data_type']]]
                print("\nQuery results：")
                print(tabulate(data, headers=headers, tablefmt="grid"))
            else:
                print("No database Find！")
                
                
                #range Query
        elif choice == '3':  
            start_key = input("Enter the Start time: (Format: YYYY-MM-DD HH:MM:SS): ")
            end_key = input("Enter the End time (Format: YYYY-MM-DD HH:MM:SS): ")
            result = bpt.range_query_with_aggregation(start_key, end_key)

            # Output Query results 
            if result['data']:
                headers = ["Timestamp", "Sensor ID", "Value", "Location", "Data Type"]
                data = [
                    [record[0], record[1]['sensor_id'], record[1]['value'], record[1]['location'], record[1]['data_type']]
                    for record in result['data']
                ]
                print(f"\nRange {start_key} to {end_key} Query results：")
                print(tabulate(data, headers=headers, tablefmt="grid"))

                # results
                aggregation_data = [
                    ["  <<SUM>>  ", result['aggregation']['total']],
                    ["<<Average>>", f"{result['aggregation']['average']:.2f}"],
                    ["  <<Min>>  ", result['aggregation']['min']],
                    ["  <<Max>>  ", result['aggregation']['max']],
                    
                    ["-" * 12, "-" * 20],  # Lines
                    
                    ["<<Min_time>>", result['aggregation']['min_time']],
                    ["<<Max_time>>", result['aggregation']['max_time']],
                ]
                print("\nRange aggregation results：")
                print(tabulate(aggregation_data, headers=["Aggregation Type", "Value"], tablefmt="fancy_grid"))
            else:
                print(f"Range {start_key} to {end_key} no Find。")
                
                
                
                
        elif choice == '4':
            start_key = input("Enter the range start time: (Format: YYYY-MM-DD HH:MM:SS): ")
            end_key = input("Enter the start time (Format: YYYY-MM-DD HH:MM:SS): ")
            bpt.delete_range(start_key, end_key)
            print(f"Range {start_key} to {end_key} Deleted！")
            
            
            
            
        elif choice == '5':
            try:
                
                
                sensor_id = int(input("Inpute Sensor ID (5-number(xxxxx)): "))
                result = bpt.query_by_id(sensor_id)

                if result:
                    print(f"\nSensor ID  {sensor_id} ：")
                    bpt.format_output(result)
                else:
                    print(f"No Find Sensor ID {sensor_id}  database。")
            except ValueError:
                print("Sensor ID(xxxxx)，input agine！")
        
        
        #Performance Test
        elif choice == '6':  
            num_records = int(input("Input test number: "))
            
            performance_test(BPlusTree, num_records) 
            
            
        elif choice == '7':
            print("Displays the first 20 pieces of data in the database：")
            display_database_records(database_path, table_name, limit=20)
            
        
              
            
        elif choice == '0':
            print("EXIT！")
            sys.exit()
        else:
            print("No Options, re-enter！")

if __name__ == "__main__":
    main()
