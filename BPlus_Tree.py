import sqlite3

class BPlusTreeNode:
    def __init__(self, leaf=False):
        self.leaf = leaf  
        self.keys = []  
        self.children = []  
        self.next_leaf = None  
        self.id_index = {} 

class BPlusTree:
    def __init__(self, order=20, database_path=None, table_name="sensor_data"):
        self.root = BPlusTreeNode(leaf=True)  
        self.order = order  
        self.database_path = database_path  
        self.table_name = table_name  
        self.id_index = {} 
        if self.database_path:
            self._initialize_database()  
            self._initialize_secondary_index()  

    def _initialize_database(self):
        
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                timestamp TEXT PRIMARY KEY,
                sensor_id TEXT,
                value REAL,
                location TEXT,
                data_type TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _initialize_secondary_index(self):
        
        if not self.database_path:
            return
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT timestamp, sensor_id, value, location, data_type FROM {self.table_name}")
        records = cursor.fetchall()
        conn.close()

        # Iterate over all records and populate the secondary index
        for timestamp, sensor_id, value, location, data_type in records:
            #Sensor_ID Not in secondary search , Creat New B+_Tree
            if sensor_id not in self.id_index:
                self.id_index[sensor_id] = BPlusTree(order=self.order)
            # secondary B+ Tree
            self.id_index[sensor_id]._insert_non_full(self.id_index[sensor_id].root, timestamp, {
                'timestamp': timestamp,
                'sensor_id': sensor_id,
                'value': value,
                'location': location,
                'data_type': data_type
            })

    def insert(self, timestamp, sensor_id, value, location, data_type):
       
        self._insert_non_full(self.root, timestamp, {
            'timestamp': timestamp,
            'sensor_id': sensor_id,
            'value': value,
            'location': location,
            'data_type': data_type
        })

        # Insert secondary B+Tree
        if sensor_id not in self.id_index:
            self.id_index[sensor_id] = BPlusTree(order=self.order)
        self.id_index[sensor_id]._insert_non_full(self.id_index[sensor_id].root, timestamp, {
            'timestamp': timestamp,
            'sensor_id': sensor_id,
            'value': value,
            'location': location,
            'data_type': data_type
        })

    
        if self.database_path:
            self._insert_into_database(timestamp, sensor_id, value, location, data_type)

    def _insert_non_full(self, node, key, value):
       
        if node.leaf:
            # nodes insert
            idx = 0
            while idx < len(node.keys) and key > node.keys[idx][0]:
                idx += 1
            node.keys.insert(idx, (key, value))
        else:
            
            idx = 0
            while idx < len(node.keys) and key > node.keys[idx][0]:
                idx += 1
            if idx < len(node.children) and len(node.children[idx].keys) == (self.order - 1):
                self._split_child(node, idx)
                if idx < len(node.keys) and key > node.keys[idx][0]:
                    idx += 1
            self._insert_non_full(node.children[idx], key, value)

    def _split_child(self, parent, index):
        
        full_node = parent.children[index]
        new_node = BPlusTreeNode(leaf=full_node.leaf)
        mid_index = (self.order - 1) // 2
        if full_node.leaf:
            new_node.keys = full_node.keys[mid_index + 1:]
            full_node.keys = full_node.keys[:mid_index + 1]
            new_node.next_leaf = full_node.next_leaf
            full_node.next_leaf = new_node
        else:
            parent.keys.insert(index, full_node.keys[mid_index])
            new_node.keys = full_node.keys[mid_index + 1:]
            full_node.keys = full_node.keys[:mid_index]
            new_node.children = full_node.children[mid_index + 1:]
            full_node.children = full_node.children[:mid_index + 1]
        parent.children.insert(index + 1, new_node)

    def _insert_into_database(self, key, sensor_id, value, location, data_type):
        
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute(f"""
            INSERT INTO {self.table_name} (timestamp, sensor_id, value, location, data_type)
            VALUES (?, ?, ?, ?, ?)
        """, (key, sensor_id, value, location, data_type))
        conn.commit()
        conn.close()

    def query_by_id(self, sensor_id):
        
       
        if sensor_id in self.id_index:
            records = self.id_index[sensor_id].range_query("0000-00-00 00:00:00", "9999-12-31 23:59:59")
            return [(timestamp, {'sensor_id': sensor_id, **value}) for timestamp, value in records]
        else:
            return []

    def range_query(self, start_key, end_key):
        
        result = []
        node = self._find_leaf_node(start_key)
        while node is not None:
            for key, value in node.keys:
                if start_key <= key <= end_key:
                    result.append((key, value))
                elif key > end_key:
                    return result
            node = node.next_leaf
        return result

    def search(self, key):
       
        node = self._find_leaf_node(key)  
        for k, v in node.keys:
            if k == key:
                return v  
        return None  

    
    def _find_leaf_node(self, key):
       
        node = self.root
        while not node.leaf:
            idx = 0
            while idx < len(node.keys) and key > node.keys[idx][0]:
                idx += 1
            node = node.children[idx]
        return node
    
    
    
    def range_query_with_aggregation(self, start_key, end_key):
        
        
        results = self.range_query(start_key, end_key)

        if not results:
            return {
                'data': [],
                'aggregation': {
                    'total': 0,
                    'average': 0,
                    'min': None,
                    'max': None,
                    'min_time': None,
                    'max_time': None,
                },
            }

        
        values = [(record[1]['value'], record[0]) for record in results]
        total = sum(value for value, _ in values)
        avg = total / len(values)
        min_value, min_time = min(values, key=lambda x: x[0])
        max_value, max_time = max(values, key=lambda x: x[0])
        
        
        return {
            'data': results,
            'aggregation': {
                'total': total,
                'average': avg,
                'min': min_value,
                'max': max_value,
                'min_time': min_time,
                'max_time': max_time,
            },
        }
        
        
        
    #Update B+ Tree
        
    def delete_range(self, start_key, end_key):
    
        
        node = self._find_leaf_node(start_key)
        while node is not None:
            new_keys = []
            for key, value in node.keys:
                if start_key <= key <= end_key:
                    continue  
                new_keys.append((key, value))
            node.keys = new_keys

            
            if node.keys and node.keys[-1][0] > end_key:
                break
            node = node.next_leaf

        
        if self.database_path:
            self._delete_from_database_range(start_key, end_key)

        
        self._delete_from_secondary_index(start_key, end_key)

    def _delete_from_database_range(self, start_key, end_key):
        
        #delete Range data
        conn = sqlite3.connect(self.database_path)
        cursor = conn.cursor()
        cursor.execute(f"""
            DELETE FROM {self.table_name}
            WHERE timestamp BETWEEN ? AND ?
        """, (start_key, end_key))
        conn.commit()
        conn.close()

    def _delete_from_secondary_index(self, start_key, end_key):
        
        
        for sensor_id in list(self.id_index.keys()):
            node = self.id_index[sensor_id]._find_leaf_node(start_key)
            while node is not None:
                new_keys = []
                for key, value in node.keys:
                    if start_key <= key <= end_key:
                        continue  
                    new_keys.append((key, value))
                node.keys = new_keys

                if node.keys and node.keys[-1][0] > end_key:
                    break
                node = node.next_leaf

            if not self.id_index[sensor_id].root.keys:
                del self.id_index[sensor_id]
      
      
            
    def format_output(self, records):
        
        if not records:
            print("\nNo Find！")
            return

        print("\n+---------------------+------------+-------+---------+-----------+")
        print(f"| {'timestamp':<19} | {'sensor_id':<10} | {'value':<5} | {'location':<7} | {'data_type':<9} |")
        print("+---------------------+------------+-------+---------+-----------+")
        for timestamp, data in records:
            print(f"| {timestamp:<19} | {data['sensor_id']:<10} | {data['value']:<5} | {data['location']:<7} | {data['data_type']:<9} |")
        print("+---------------------+------------+-------+---------+-----------+")
