import requests
from CallAPI import CallAPI


class CallAPIGET(CallAPI):

    def execute_api(self, url, body):
        response = requests.get(url)
        return response
