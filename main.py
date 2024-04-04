import os
import time

# local imports
from services.first_sync import FirstSync
from services.map_table import MapTable
from services.table_manager import TableManager


def read_env_in_memory():
    env = {}
    with open('.env') as f:
        for line in f:
            key, value = line.strip().split('=')
            env[key] = value
    return env


def process_first_run(env):
    sync = FirstSync(env)
    sync.sync_records()
    env['FIRST_RUN'] = '1'
    # update the value of FIRST_RUN in the .env file
    with open('.env', 'w') as f:
        for key, value in env.items():
            f.write(f"{key}={value}\n")


def main():
    # Read the environment variables
    env = read_env_in_memory()
    table_map = TableManager(env).get_table_columns_map()
    while True:
        if env['FIRST_RUN'] == '0':
            process_first_run(env)
        else:
            data = MapTable(queries=queries, env=env).post_data
            print(data)

            # Send the data to the API here

        time.sleep(int(env['SYNC_INTERVAL']))  # sleep for 60 seconds # should updated to use configured time


if __name__ == "__main__":
    main()
