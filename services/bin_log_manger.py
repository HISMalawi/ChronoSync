import os
import re

class BinLogManager:
    def __init__(self, bin_log_folder):
        self.bin_log_folder = bin_log_folder

    def process_next_file(self):
        # Get the list of bin log files in the folder
        bin_log_files = os.listdir(self.bin_log_folder)
        pattern = r"(.*)-bin.(\d+)"
        bin_log_files = [file for file in bin_log_files if re.match(pattern, file)]
        bin_log_files.sort()
        bin_log_pop()

        if bin_log_files:
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
            result = subprocess.check_output(f"mysqlbinlog -v {file_path} | grep '###'", shell=True)
            if result:
                print(f"Processing this content: {result}")
        # catch all exceptions
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")
            return False
        return True
    
    def parse_data(data):
        operations = re.split(r'### UPDATE|### INSERT INTO|### DELETE', data)
        parsed_data = []
        current_operation = {}
        operation_types = ["UPDATE", "INSERT INTO", "DELETE"]

        for i, operation in enumerate(operations[1:], start=1):
            operation = operation.strip()
            if i < len(operations) - 1:  # if not the last operation
                operation, next_operation_type = re.split(r'### UPDATE|### INSERT INTO|### DELETE', operation)
            current_operation = {"operation": operation_types[i % len(operation_types)] + operation}
            lines = operation.split("\n")
            for line in lines:
                if line.startswith("WHERE") or line.startswith("SET"):
                    current_operation[line] = {}
                elif line.startswith("@"):
                    key, value = line.split("=")
                    current_operation[list(current_operation.keys())[-1]][key] = value.strip()
            parsed_data.append(current_operation)

        return parsed_data