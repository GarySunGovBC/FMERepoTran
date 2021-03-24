import requests
from FMEAPI.CallAPI import CallAPI


class CallAPIPOST(CallAPI):

    def __init__(self, api_config, server, token, log):
        super().__init__(api_config, server, token, log)
        self.http_method = "POST"

    def execute_api(self, url, headers, body):
        response = requests.post(url=url, data=body, headers=headers)
        return response

    def call_api_upload(self, method, files, url_params=None, return_codes=None, headers=None):
        url = self.populate_url(method, url_params)
        response = requests.post(url=url, headers=headers, files=files)
        return self.check_response(response, url, return_codes)
