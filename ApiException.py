import json


class APIException(Exception):

    def __init__(self, url, response):
        self.error = {"url": url, "status_code": response.status_code}
        msg = json.loads(response.text)
        if len(msg) > 0:
            self.error["message"] = msg["message"]
