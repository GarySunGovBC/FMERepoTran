import json


class APIException(Exception):

    def __init__(self, url, method, response):
        self.error = {"url": url, "method": method, "status_code": response.status_code}
        msg = json.loads(response.text)
        text = "No message"
        if len(msg) > 0:
            if "message" in msg:
                text = msg["message"]
            else:
                text = json.dumps(msg)
        self.error["message"] = "Call API failed. url: %s, method: %s, status code: %s, error: %s" % (
            self.error["url"], self.error["method"], self.error["status_code"], text)
