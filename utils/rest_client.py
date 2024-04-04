import requests

class RestClient:
    def __init__(self, base_url):
        self.base_url = base_url
        self.token = None

    def get(self, endpoint, params=None, headers=None):
        url = self.base_url + endpoint
        # if token is available, add it to the headers under Authorization
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        response = requests.get(url, params=params, headers=headers)
        if response.status_code not in range(200, 300):
            raise Exception(f'HTTP error {response.status_code}: {response.text}')
        return response.json()

    def post(self, endpoint, data=None, headers=None):
        url = self.base_url + endpoint
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        response = requests.post(url, json=data, headers=headers)
        if response.status_code not in range(200, 300):
            raise Exception(f'HTTP error {response.status_code}: {response.text}')
        return response.json()

    def put(self, endpoint, data=None, headers=None):
        url = self.base_url + endpoint
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        response = requests.put(url, json=data, headers=headers)
        if response.status_code not in range(200, 300):
            raise Exception(f'HTTP error {response.status_code}: {response.text}')
        return response.json()

    def delete(self, endpoint, headers=None):
        url = self.base_url + endpoint
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'
        response = requests.delete(url, headers=headers)
        if response.status_code not in range(200, 300):
            raise Exception(f'HTTP error {response.status_code}: {response.text}')
        return response.json()
    
    def set_token(self, token):
        self.token = token