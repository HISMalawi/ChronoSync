import os
import configparser
import subprocess

def check_folder_permissions():
    # Check if the folder exists
    folder = '/etc/mysql/conf.d/'
    if not os.path.exists(folder):
        return

    # Check if the folder has the correct permissions
    permissions = oct(os.stat(folder).st_mode & 0o777)
    if permissions != '0o700':
        # prompt the user to put their password to change the permissions
        subprocess.run(['sudo', 'chmod', '777', folder], check=True)

def update_mysql_config():
    check_folder_permissions()
    config_file = '/etc/mysql/conf.d/my.cnf'
    config = configparser.ConfigParser(allow_no_value=True)
    
    # Check if the file exists
    if os.path.exists(config_file):
        config.read(config_file)
    else:
        config.add_section('mysqld')
    
    # Update the keys
    keys = {
        'bind-address': '0.0.0.0',
        'log-bin': None,
        'sql_mode': 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION',
        'server_id': '1',
        'max_binlog_size': '30M',
        'binlog_format': 'row'
    }
    
    for key, value in keys.items():
        if not config.has_option('mysqld', key):
            config.set('mysqld', key, value)
        else:
            print(f"Key '{key}' already exists. Skipping...")
    
    # Write the changes back to the file

    with open(config_file, 'w') as configfile:
        config.write(configfile)

    # Restart the MySQL service
    subprocess.run(['sudo', 'systemctl', 'restart', 'mysql'], check=True)