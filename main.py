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
            # Replace the queries with the actual logs
            queries = [
                {
                    "operation": "UPDATE",
                    "database": "zomba",
                    "table": "concept_name",
                    "data": {
                        "@1": "183",
                        "@2": "pistne",
                        "@3": "en",
                        "@4": "1",
                        "@5": "2004-01-01 00:00:00",
                        "@6": "191",
                        "@7": "0",
                        "@8": "1",
                        "@9": "NULL",
                        "@10": "NULL",
                        "@11": "b99728b2-8d80-11d8-abbb-0024217bb78e",
                        "@12": "FULLY_SPECIFIED",
                        "@13": "0"
                    }
                },
                {
                    "operation": "INSERT",
                    "database": "zomba",
                    "table": "concept_name",
                    "data": {
                        "@1": "183",
                        "@2": "manzy",
                        "@3": "",
                        "@4": "1",
                        "@5": "2024-04-04 12:57:23",
                        "@6": "106360",
                        "@7": "0",
                        "@8": "NULL",
                        "@9": "NULL",
                        "@10": "NULL",
                        "@11": "e13d76ef-f282-11ee-a254-0242ac110005",
                        "@12": "NULL",
                        "@13": "0"
                    }
                },
                {
                    "operation": "DELETE",
                    "database": "zomba",
                    "table": "concept_name",
                    "data": {
                        "@1": "183",
                        "@2": "manzy",
                        "@3": "",
                        "@4": "1",
                        "@5": "2024-04-04 12:57:23",
                        "@6": "106360",
                        "@7": "0",
                        "@8": "NULL",
                        "@9": "NULL",
                        "@10": "NULL",
                        "@11": "e13d76ef-f282-11ee-a254-0242ac110005",
                        "@12": "NULL",
                        "@13": "0"
                    }
                }
            ]
            print('Syncing records... using bin logs')
            data = MapTable(queries=queries, env=env).post_data
            print(data)

            # Send the data to the API here

        time.sleep(int(env['SYNC_INTERVAL']))  # sleep for 60 seconds # should updated to use configured time


if __name__ == "__main__":
    main()
