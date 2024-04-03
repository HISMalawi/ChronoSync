import mysql.connector

class FirstSync:
    def __init__(self, env):
        self.host = env['HOST']
        self.user = env['USER']
        self.password = env['PASSWORD']
        self.database = env['DATABASE']
        self.port = env['PORT']
        self.tables = env['TRANS_TABLE'].split(',')
        self.transform = env['TRANSFORM'] == '1' # Convert to boolean

    def sync_records(self):
        # Connect to the database
        connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )

        # Create a cursor object to execute SQL queries
        cursor = connection.cursor(dictionary=True)

        # Loop through the tables and sync the records
        for table in self.tables:
            self.table = table
            self.sync_table(cursor)

        # Close the database connection
        cursor.close()
        connection.close()

    def handle_new_records_query(self):
        # we need to create a special query if the table in question is drug_order otherwise we can use the normal query
        if self.table == 'drug_order':
            query = f"SELECT * FROM {self.table} do INNER JOIN orders o ON do.order_id = o.order_id WHERE o.date_created >= '2024-01-01 00:00:00' AND o.date_created <= NOW()"
        elif self.table == 'merge_audits' or self.table == 'pharmacy_stock_balances':
            query = f"SELECT * FROM {self.table} WHERE created_at >= '2024-01-01 00:00:00' AND created_at <= NOW()"
        else:
            query = f"SELECT * FROM {self.table} WHERE date_created >= '2024-01-01 00:00:00' AND date_created <= NOW()"
        return query

    def handle_modified_records_query(self):
        if self.table == 'merge_audits' or self.table == 'pharmacy_stock_balances':
            return f"SELECT * FROM {self.table} WHERE updated_at >= '2024-01-01 00:00:00' AND updated_at <= NOW() AND created_at < '2024-01-01 00:00:00'"
        else:
            return f"SELECT * FROM {self.table} WHERE date_changed >= '2024-01-01 00:00:00' AND date_changed <= NOW() AND date_created < '2024-01-01 00:00:00'"

    def sync_table(self, cursor):
        # Sync new records
        self.sync_new_records(cursor)
        self.sync_modified_records(cursor)
    
    def sync_new_records(self, cursor):
        if self.table in ['pharmacies', 'cohort_member']:
            return
        query = self.handle_new_records_query()
        cursor.execute(query)
        records = cursor.fetchall()
        records_to_sync = []

        if self.transform:
            records_to_sync = self.transform_records(records)

        # Sync the transformed records to the central server
        self.sync_to_server(records_to_sync)

    def sync_modified_records(self, cursor):
        if self.table in ['relationship', 'cohort_member', 'pharmacy_obs', 'pharmacies', 'drug_order', 'obs', 'orders', 
                          'patient_identifier', 'person_address']:
            return
        query = self.handle_modified_records_query()
        cursor.execute(query)
        records = cursor.fetchall()
        records_to_sync = []

        if self.transform:
            records_to_sync = self.transform_records(records)

        # Sync the transformed records to the central server
        self.sync_to_server(records_to_sync)

    def transform_records(self, records):
        # Perform your transformations on the records here
        transformed_records = []

        # Example transformation: append site_id to each record the record will not have such a column
        for record in records:
            record['site_id'] = 1
            transformed_records.append(record)
            print(record)

        return transformed_records

    def sync_to_server(self, records):
        # Sync the records to the central server here
        print(f"Syncing {len(records)} records to the central server...")