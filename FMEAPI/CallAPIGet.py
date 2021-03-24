import requests
from FMEAPI.CallAPI import CallAPI


class CallAPIGET(CallAPI):

    def execute_api(self, url, headers, body):
        response = requests.get(url=url, data=body, headers=headers)
        return response
