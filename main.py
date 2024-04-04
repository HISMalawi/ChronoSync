import os
import time

# local imports
from services.first_sync import FirstSync
from services.map_table import MapTable

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
    while True:
        if env['FIRST_RUN'] == '0':
            process_first_run(env)
        else:
            queries = [
                "INSERT INTO drug_order VALUES (?, ?, ?, ?) WHERE @2 = 23)",
                "UPDATE encounter SET obs_name = 'name here' WHERE @3 = 1 ",
                "DELETE FROM person_address WHERE @1 = 1"
            ]
            print('Syncing records... using bin logs')
            MapTable(queries=queries, env=env)
        time.sleep(int(env['SYNC_INTERVAL']))  # sleep for 60 seconds # should updated to use configured time

if __name__ == "__main__":
    main()