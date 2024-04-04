import re

import mysql.connector as mysql


def table_mapper(table_name):
    tables = [
        "drug_order",
        "encounter",
        "obs",
        "orders",
        "patient",
        "patient_identifier",
        "patient_program",
        "patient_state",
        "person",
        "person_address",
        "person_attribute",
        "person_name",
        "pharmacies",
        "pharmacy_batch_item_reallocations",
        "pharmacy_batch_items",
        "pharmacy_batches",
        "pharmacy_obs",
        "pharmacy_stock_balances",
        "pharmacy_stock_verifications",
        "relationship",
        "merge_audits",
        "note",
        "cohort_member"
    ]
    if table_name in tables:
        return True
    else:
        return False


def connectTest():
    try:
        db = mysql.connect(
            host="",
            user="",
            password="",
            database=""
        )
        return db

    except Exception as e:
        # logg the error
        print(e)
        return None


# Read binary file
queries = [
    "INSERT INTO drug_order VALUES (?, ?, ?, ?) WHERE @2 = 23)",
    "UPDATE encounter SET obs_name = 'name here' WHERE @3 = 1 ",
    "DELETE FROM person_address WHERE @1 = 1"
]


# Map columns
def map_columns(table_name):
    test_conn = connectTest()
    cursor = test_conn.cursor()

    # Get columns for the specified table
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    columns_info = cursor.fetchall()

    # Create a hashmap of column names
    column_hashmap = {}
    for index, column_info in enumerate(columns_info, start=1):
        modified_index = f"@{index}"
        column_hashmap[modified_index] = column_info[0]  # Column name

    test_conn.close()
    return column_hashmap


# Extract table name
def extract_table_name(query):
    # Match INSERT INTO table_name or DELETE FROM table_name or UPDATE table_name
    match = re.search(r'(?:INSERT INTO|DELETE FROM|UPDATE)\s+(\w+)', query)
    if match:
        return match.group(1)
    return None


# Map query with column
def map_columns_to_query(sql_query, column_map):
    mapped_query = sql_query

    # Replace placeholders with column names
    for key, value in column_map.items():
        mapped_query = mapped_query.replace(str(key), value)

    print(mapped_query)
    return mapped_query


# Check table name
def check_table_name(table_name):
    return table_mapper(table_name)


# Check if it's an insert query
for query in queries:
    # Extract table name from the query string directly
    table_name = extract_table_name(query)
    if table_name:
        print("Table name:", table_name)
    else:
        print("No table name found in query:", query)

    if check_table_name(table_name):
        # Get table desc
        hashmap = map_columns(table_name)
        map_columns_to_query("SELECT * FROM 1 WHERE 2 = 'this'", hashmap)