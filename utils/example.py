import os
import re
import json

def parse_data(data):
    print(f"Data: {data}")
    operations = re.split(r'### UPDATE|### INSERT INTO|### DELETE FROM', data)
    parsed_data = []
    operation_types = ["UPDATE ", "INSERT INTO ", "DELETE FROM "]

    for i, operation in enumerate(operations[1:], start=1):
        operation = operation.replace('###', '').strip()
        current_operation = {"operation": operation_types[i % len(operation_types)] + operation}
        lines = operation.split("\n")
        for line in lines:
            if line.startswith("WHERE") or line.startswith("SET"):
                current_operation[line] = {}
            elif line.startswith("@"):
                key, value = line.split("=")
                current_operation[list(current_operation.keys())[-1]][key] = value.strip()
        parsed_data.append(current_operation)

    return json.dumps(parsed_data, indent=4)

def main():
    data = """
### UPDATE `zomba`.`concept_name`
### WHERE
###   @1=183
###   @2='pistone'
###   @3='en'
###   @4=1
###   @5='2004-01-01 00:00:00'
###   @6=191
###   @7=0
###   @8=1
###   @9=NULL
###   @10=NULL
###   @11='b99728b2-8d80-11d8-abbb-0024217bb78e'
###   @12='FULLY_SPECIFIED'
###   @13=0
### SET
###   @1=183
###   @2='pistne'
###   @3='en'
###   @4=1
###   @5='2004-01-01 00:00:00'
###   @6=191
###   @7=0
###   @8=1
###   @9=NULL
###   @10=NULL
###   @11='b99728b2-8d80-11d8-abbb-0024217bb78e'
###   @12='FULLY_SPECIFIED'
###   @13=0
### INSERT INTO `zomba`.`concept_name`
### SET
###   @1=183
###   @2='manzy'
###   @3=''
###   @4=1
###   @5='2024-04-04 12:57:23'
###   @6=106360
###   @7=0
###   @8=NULL
###   @9=NULL
###   @10=NULL
###   @11='e13d76ef-f282-11ee-a254-0242ac110005'
###   @12=NULL
###   @13=0
### DELETE FROM `zomba`.`concept_name`
### WHERE
###   @1=183
###   @2='manzy'
###   @3=''
###   @4=1
###   @5='2024-04-04 12:57:23'
###   @6=106360
###   @7=0
###   @8=NULL
###   @9=NULL
###   @10=NULL
###   @11='e13d76ef-f282-11ee-a254-0242ac110005'
###   @12=NULL
###   @13=0
"""
    print(parse_data(data))

if __name__ == "__main__":
    main()