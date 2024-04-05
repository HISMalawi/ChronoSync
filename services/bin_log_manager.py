import os
import re
import json
import subprocess

from utils.connector import fetch_cursor
from utils.location import fetch_site_id
from .sync_manager import SyncManager
from .log_converter import process_list

class BinLogManager:
    def __init__(self, env, table_columns_map):
        self.env = env
        self.bin_log_folder = env['LOG_PATH']
        self.transform_data = env['TRANSFORM'] == '1'
        result = fetch_cursor(self.env)
        cursor = result[0]
        connection = result[1]
        self.site_id = fetch_site_id(cursor)
        self.table_columns_map = table_columns_map
        connection.close()


    def process_logs(self):
        # Get the list of bin log files in the folder
        bin_log_files = os.listdir(self.bin_log_folder)
        pattern = r"(.*)-bin.(\d+)"
        bin_log_files = [file for file in bin_log_files if re.match(pattern, file)]
        # skip files that end with 000001 and 000002
        bin_log_files = [file for file in bin_log_files if not file.endswith('000001') and not file.endswith('000002')]
        bin_log_files.sort()

        if bin_log_files:
            bin_log_files.pop()
            for file in bin_log_files:
                file_path = os.path.join(self.bin_log_folder, file)
                result = self.process_file(file_path)
                # if result is True, then continue processing. Otherwise completely stop processing
                if not result:
                    break
        else:
            print("No bin log files to process.")

    def process_file(self, file_path):
        try:
            result = subprocess.check_output(f"mysqlbinlog -v {file_path} | grep '### '", shell=True)
            if result:
                afcon = str(result.decode('utf-8'))
                data = self.parse_data(afcon)
                SyncManager(self.env).process_data(process_list(data, self.table_columns_map))
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            return False
        return True
    
    def parse_data(data):
        operations = re.split(r'### UPDATE|### INSERT INTO|### DELETE FROM', data)
        parsed_data = []
        operation_types = ["UPDATE", "INSERT", "DELETE"]

        for i, operation in enumerate(operations[1:]):
            op_value = operation_types[i % len(operation_types)]
            result = []
            if 'SET' in operation:
                result = operation.replace('###', '').replace('\n', '').strip().split('SET')
            elif 'WHERE' in operation:
                result = operation.replace('###', '').replace('\n', '').strip().split('WHERE')
            else:
                # throw an error because this will be unexpected
                raise ValueError("Operation does not contain SET or WHERE")
            database_name = result[0].split('`')[1]
            table_name = result[0].split('`')[3]
            gems = result[1].strip().split('@')
            data = {}
            if self.transform_data:
                data["site_id"] = self.site_id
            for i, gem in enumerate(gems):
                if gem:
                    key, value = gem.split('=')
                    data[f"@{key}"] = value.strip().strip("'").strip('"')
            current_operation = {"operation": op_value, "database": database_name, "table": table_name, "data": data}
            parsed_data.append(current_operation)
        
        return json.dumps(parsed_data, indent=1)