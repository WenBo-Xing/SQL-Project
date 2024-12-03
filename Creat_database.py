import sqlite3
import os
import random
import datetime


database_path = '/Users/xingwenbo/Desktop/4525_FinalProject/sensor_data.db'

if os.path.exists(database_path):
    os.remove(database_path)

conn = sqlite3.connect(database_path)
cursor = conn.cursor()


cursor.execute('''
CREATE TABLE sensor_data (
    timestamp TIMESTAMP NOT NULL,
    sensor_id INTEGER NOT NULL,
    value REAL NOT NULL,
    location TEXT NOT NULL,
    data_type TEXT NOT NULL,
    PRIMARY KEY (timestamp, sensor_id)
)
''')


sample_data = []

total_records = 10000  
sensor_count = min(total_records, 90000) 
sensor_ids = random.sample(range(10000, 10000 + sensor_count), sensor_count)  


locations = ['Field_1', 'Field_2', 'Field_3', 'Field_4', 'Field_5', 'Field_6', 'Field_7', 'Field_8', 'Field_9']
data_types = ['Temp', 'Humidity', 'Light']
start_time = datetime.datetime(2024, 1, 1, 1, 0, 0)


for i in range(total_records):
    timestamp = start_time + datetime.timedelta(minutes=5 * i)  
    sensor_id = random.choice(sensor_ids)
    value = round(random.uniform(20.0, 80.0), 1)
    location = random.choice(locations)
    data_type = random.choice(data_types)
    sample_data.append((timestamp.strftime('%Y-%m-%d %H:%M:%S'), sensor_id, value, location, data_type))

cursor.executemany('''
    INSERT OR IGNORE INTO sensor_data (timestamp, sensor_id, value, location, data_type)
    VALUES (?, ?, ?, ?, ?)
    ''', sample_data)


cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON sensor_data (timestamp);')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_data_type ON sensor_data (data_type);')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_sensor_id ON sensor_data (sensor_id);')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp_sensor_id ON sensor_data (timestamp, sensor_id);')

   
cursor.execute('''
    CREATE VIEW IF NOT EXISTS hourly_average_view AS
    SELECT
        strftime('%Y-%m-%d %H:00:00', timestamp) AS hour,
        AVG(value) AS avg_value
    FROM sensor_data
    GROUP BY strftime('%Y-%m-%d %H:00:00', timestamp)
    ORDER BY hour;
    ''')

  
cursor.execute('''
    CREATE VIEW IF NOT EXISTS daily_average_view AS
    SELECT
        strftime('%Y-%m-%d', timestamp) AS day,
        AVG(value) AS avg_value
    FROM sensor_data
    GROUP BY strftime('%Y-%m-%d', timestamp)
    ORDER BY day;
    ''')


cursor.execute('''
    CREATE TRIGGER IF NOT EXISTS maintain_order_trigger
    BEFORE INSERT ON sensor_data
    FOR EACH ROW
    
    WHEN NEW.timestamp <= (SELECT MAX(timestamp) FROM sensor_data WHERE sensor_id = NEW.sensor_id)
    BEGIN
        SELECT RAISE(ABORT, 'Timestamp for new data must be greater than the latest timestamp for the given sensor.');
    END;
    ''')

  
cursor.execute('''
    CREATE TABLE IF NOT EXISTS hourly_averages (
        hour TEXT PRIMARY KEY,
        avg_value REAL
    )
    ''')

    
cursor.execute('''
    CREATE TRIGGER IF NOT EXISTS update_hourly_avg_trigger
    AFTER INSERT ON sensor_data
    FOR EACH ROW
    BEGIN
        INSERT INTO hourly_averages (hour, avg_value)
        VALUES (
            strftime('%Y-%m-%d %H:00:00', NEW.timestamp),
            (SELECT AVG(value) FROM sensor_data WHERE strftime('%Y-%m-%d %H:00:00', timestamp) = strftime('%Y-%m-%d %H:00:00', NEW.timestamp))
        )
        ON CONFLICT(hour) DO UPDATE SET
            avg_value = (SELECT AVG(value) FROM sensor_data WHERE strftime('%Y-%m-%d %H:00:00', timestamp) = strftime('%Y-%m-%d %H:00:00', NEW.timestamp));
    END;
    ''')

    
cursor.execute('''
    CREATE TABLE IF NOT EXISTS delete_schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        last_deleted TIMESTAMP
    )
    ''')

   
def delete_old_data():
    cursor.execute('''
        DELETE FROM sensor_data
        WHERE timestamp < datetime('now', '-1 year')
        ''')
conn.commit()
print("Expired data has been deleted。")


delete_old_data()


conn.close()

print(f"\nDatabase Save at: {database_path}")
