from utils.rest_client import RestClient

class SyncManager:
    def __init__(self, env):
        self.base_url = env['CDR_URL']
        self.username = env['CDR_USERNAME']
        self.password = env['CDR_PASSWORD']
        self.client = RestClient()

    def process_data(self, data):
        try:
            # Initiate login using rest_client
            self.login()

            # Push the data to the remote server
            # check whether the data is a list or a single object
            if isinstance(data, list):
                for item in data:
                    self.process_request(item)
            else:
                self.process_request(data)
        except Exception as e:
            # Handle any exceptions here
            print(f"An error occurred: {str(e)}")
            return False

    def login(self):
        result = self.client.post('/login', data={'username': self.username, 'password': self.password})
        self.client.set_token(result['token'])

    def process_request(self, data):
        if data['operation'] == 'UPDATE':
            self.client.put('/data', data=data)
        elif data['operation'] == 'INSERT':
            self.client.post('/data', data=data)
        elif data['operation'] == 'DELETE':
            self.client.delete('/data', data=data)