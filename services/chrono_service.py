import os

def finalize_installation():
    # Enable the service
    os.system('sudo systemctl enable cdr')

    # Start the service
    os.system('sudo systemctl start cdr')

    print('Installation completed.')

def create_service():
    service_name = 'cdr'
    service_file = f'/etc/systemd/system/{service_name}.service'
    # main_script = '/path/to/your/main.py'
    # this should use pwd to get the current directory
    current_directory = os.getcwd()
    main_path = os.path.join(current_directory, '../main.py')
    main_script = os.path.abspath(main_path)

    # Check if the service file already exists
    if os.path.exists(service_file):
        print(f'Service {service_name} already exists.')
        return

    # Define the service file content
    service_content = f"""[Unit]
Description=CDR Streaming Service

[Service]
ExecStart=/usr/bin/python3 {main_script}
Restart=always

[Install]
WantedBy=multi-user.target
"""

    # Create a temporary file
    with open('temp.service', 'w') as f:
        f.write(service_content)

    # Move the temporary file to the systemd directory with sudo privileges
    os.system(f'sudo mv temp.service {service_file}')
    os.system(f'sudo chown root:root {service_file}')
    os.system(f'sudo chmod 644 {service_file}')

    print(f'Service {service_name} created.')
    finalize_installation()

if __name__ == "__main__":
    create_service()