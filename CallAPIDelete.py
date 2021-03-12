import requests
from CallAPI import CallAPI


class CallAPIDELETE(CallAPI):

    def __init__(self, app_config, server, token):
        super().__init__(app_config, server, token)
        self.http_method = "DELETE"

    def execute_api(self, url, headers, body):
        response = requests.delete(url=url, data=body, headers=headers)
        return response

