import os
import requests
import json


class CallAPI(object):

    def __init__(self, app_config, server, token):
        self.app_config = app_config
        self.server = server
        self.token = token

    def execute_api(self, url, body):
        pass

    def api_return(self, response):
        return json.loads(response.text)

    def call_api(self, method, url_params=None, body=None):
        root = self.app_config["api_root"] % self.app_config[self.server]
        url = self.app_config[method]
        if url_params:
            url = url % tuple(url_params)
        if "token=" in url:
            url = url.replace("token=", "token=" + self.token)
        url = os.path.join(root, url)
        response = self.execute_api(url, body)
        if response.ok:
            return self.api_return(response)
        msg = json.loads(response.text)
        raise Exception("Call API failed. server: %s, error:%s" % (self.app_config[self.server], msg["message"]))
