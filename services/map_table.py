import re

from utils.connector import fetch_cursor


class MapTable:
    def __init__(self, queries, env):
        self.queries = queries
        result = fetch_cursor(env)
        self.cursor = result[0]
        self.column_hashmap = {}
        self.connection = result[1]
        self.map_query()

    def map_query(self):
        for query in self.queries:
            table_name = self.extract_table_name(query)
            query = f"SHOW COLUMNS FROM {table_name}"
            self.cursor.execute(query)
            columns_info = self.cursor.fetchall()

            # Create a hashmap of column names
            for index, column_info in enumerate(columns_info, start=1):
                modified_index = f"@{index}"
                self.column_hashmap[modified_index] = column_info['Field']  # Column name
            print(self.column_hashmap)

    def extract_table_name(self, query):
        # Match INSERT INTO table_name or DELETE FROM table_name or UPDATE table_name
        match = re.search(r'(?:INSERT INTO|DELETE FROM|UPDATE)\s+(\w+)', query)
        if match:
            return match.group(1)
        return None

