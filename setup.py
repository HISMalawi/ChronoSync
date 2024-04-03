from services.mysql_configuration import update_mysql_config
from services.chrono_service import create_service

# when the file is executed, the following code will run
if __name__ == '__main__':
    update_mysql_config()
    create_service()