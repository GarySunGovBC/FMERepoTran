import requests
from FMEAPI.CallAPI import CallAPI


class CallAPIDELETE(CallAPI):

    def __init__(self, api_config, server, token, log):
        super().__init__(api_config, server, token, log)
        self.http_method = "DELETE"

    def execute_api(self, url, headers, body):
        response = requests.delete(url=url, data=body, headers=headers)
        return response
