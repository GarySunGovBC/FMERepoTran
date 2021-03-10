import requests
from CallAPI import CallAPI


class CallAPIPOST(CallAPI):

    def execute_api(self, url, body):
        response = requests.post(url=url, data=body)
        return response

    def api_return(self, response):
        return response.text
