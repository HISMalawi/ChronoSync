from utils.connector import fetch_cursor


class MapTable:
    def __init__(self, queries, env):
        self.queries = queries
        result = fetch_cursor(env)
        self.cursor = result[0]
        self.post_data = []
        self.connection = result[1]
        self.map_query()

    def map_query(self):
        for query in self.queries:
            table_name = query['table']
            print(query['table'])
            table_desc = f"SHOW COLUMNS FROM {table_name}"
            self.cursor.execute(table_desc)
            columns_info = self.cursor.fetchall()

            # Create a hashmap of column names
            column_hashmap = {}
            for index, column_info in enumerate(columns_info, start=1):
                modified_index = f"@{index}"
                column_hashmap[modified_index] = column_info['Field']  # Column name

            updated_data = {}  # Create a separate dictionary for updated data
            for key, value in query["data"].items():
                if key in column_hashmap:
                    updated_data[column_hashmap[key]] = value
                else:
                    updated_data[key] = value  # If key not found in column_hashmap, keep it as it is | handle it here

            query["data"] = updated_data  # Update the query's data dictionary

            self.post_data.append(updated_data)
        print(self.post_data)
        return self.post_data


