import os
import requests
import json


class CallAPI(object):

    def __init__(self, app_config, server, token):
        self.app_config = app_config
        self.server = server
        self.token = token

    def call_api(self, method, params=None):
        url = self.app_config["api_root"] % self.app_config[self.server]
        method = self.app_config[method]
        if params:
            for i in range(0, len(params)):
                method = method.replace("<%s>" % i, params[i])
        if "token=" in method:
            method = method % self.token
        url = os.path.join(url, method)
        response = requests.get(url)
        if response.ok:
            return json.loads(response.text)
        msg = json.loads(response.text)
        raise Exception("Call API failed. server: %s, error:%s" % (self.app_config[self.server], msg["message"]))
