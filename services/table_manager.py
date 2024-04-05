from utils.connector import fetch_cursor

class TableManager:
    def __init__(self, env):
        self.env = env
        result = fetch_cursor(env)
        self.cursor = result[0]
        self.connection = result[1]

    def get_table_columns_map(self):
        table_names = self.env['TRANS_TABLE'].split(',')
        table_columns_map = {}
        for table_name in table_names:
            self.cursor.execute(f"DESCRIBE {table_name}")
            columns = self.cursor.fetchall()
            column_dict = {}
            # now we want something like this {'@1': 'id', '@2': 'name', '@3': 'age'}
            for index, column in enumerate(columns):
                key = f"@{index + 1}"
                if column:
                    column_dict[key] = column['Field']
            table_columns_map[table_name] = column_dict
        
        return table_columns_map