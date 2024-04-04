import os
import re
import json

def parse_data(data, transform_data):
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
        if transform_data:
            data["site_id"] = 700
        for i, gem in enumerate(gems):
            if gem:
                key, value = gem.split('=')
                data[f"@{key}"] = value.strip().strip("'").strip('"')
        current_operation = {"operation": op_value, "database": database_name, "table": table_name, "data": data}
        parsed_data.append(current_operation)
    print(f"Data: {parsed_data}")
    return json.dumps(parsed_data, indent=1)

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
    print(parse_data(data, False))

if __name__ == "__main__":
    main()