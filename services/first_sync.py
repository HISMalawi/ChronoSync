import json

from utils.location import fetch_site_id
from utils.connector import fetch_cursor
from .sync_manager import SyncManager

class FirstSync:
    def __init__(self, env):
        result = fetch_cursor(env)
        self.env = env
        self.cursor = result[0]
        self.connection = result[1]
        self.tables = env['TRANS_TABLE'].split(',')
        self.transform = env['TRANSFORM'] == '1' # Convert to boolean

    def sync_records(self):
        self.site_id = fetch_site_id(self.cursor)

        # Loop through the tables and sync the records
        for table in self.tables:
            self.table = table
            self.sync_table()

        # Close the database connection
        self.cursor.close()
        self.connection.close()

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

    def sync_table(self):
        # Sync new records
        self.sync_new_records()
        self.sync_modified_records()
    
    def sync_new_records(self):
        if self.table in ['pharmacies', 'cohort_member']:
            return
        query = self.handle_new_records_query()
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        records_to_sync = []

        if self.transform:
            records_to_sync = self.process_output(self.transform_records(records), 'INSERT')
        else:
            self.process_output(records, 'INSERT')

        # Sync the transformed records to the central server
        self.sync_to_server(records_to_sync)

    def sync_modified_records(self):
        if self.table in ['relationship', 'cohort_member', 'pharmacy_obs', 'pharmacies', 'drug_order', 'obs', 'orders', 
                          'patient_identifier', 'person_address']:
            return
        query = self.handle_modified_records_query()
        self.cursor.execute(query)
        records = self.cursor.fetchall()
        records_to_sync = []

        if self.transform:
            records_to_sync = self.process_output(self.transform_records(records), 'UPDATE')
        else:
            self.process_output(records, 'UPDATE')

        # Sync the transformed records to the central server
        self.sync_to_server(records_to_sync)

    def transform_records(self, records, operation):
        # Perform your transformations on the records here
        transformed_records = []

        # Example transformation: append site_id to each record the record will not have such a column
        for record in records:
            record['site_id'] = self.site_id
            transformed_records.append(record)

        return transformed_records

    def sync_to_server(self, records):
        SyncManager(self.env).process_data(records)

    def process_output(self, records, operation):
        output = []
        for record in records:
            output.append({'operation': operation, 'database': self.env['DATABASE'], 'table': self.table, 'data': record})
        return json.dumps(output, indent=1)