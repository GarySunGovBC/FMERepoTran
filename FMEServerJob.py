import json

import FMEAPI
from ApiException import APIException


class FMEServerJob(object):

    def __init__(self, app_config, key):
        self.job_config_file = "job.json"
        self.key = key
        self.app_config = app_config
        with open(self.job_config_file) as job_config_json:
            self.job_config = json.load(job_config_json)
            self.repo_filter = self.job_config["repo_filter"]
        self.job = FMEAPI.FmeApis(app_config, key + "_fme_server", app_config[key + "_token"])
        ok = self.job.check_health()
        if not ok:
            raise Exception("FME Server is not healthy.")

    def list_repo(self):
        repo_list = self.job.list_repos()
        return repo_list

    def list_repo_fmw(self):
        result = {}
        repo_list = self.job.list_repos()
        dict_key = None
        dict_value = []
        for repo in repo_list:
            if self.repo_filter:
                if repo["name"] not in self.repo_filter:
                    continue
            if dict_key:
                result[dict_key] = dict_value
            dict_key = repo["name"]
            print(dict_key)
            dict_value = []
            fmw_list = self.job.list_repo_sub_items(dict_key)
            for fmw in fmw_list:
                print(fmw["name"])
                try:
                    datasets = self.job.get_repo_datasets(dict_key, fmw["name"])
                    parameters = self.job.list_repo_parameters(dict_key, fmw["name"])
                    resources = self.job.list_repo_resources(dict_key, fmw["name"])
                    services = self.job.list_repo_services(dict_key, fmw["name"])
                except APIException as e:
                    datasets = e.error
                props = {"datasets": datasets, "parameters": parameters, "resources": resources, "services": services}
                print(props)
                dict_value.append({fmw["name"]: props})
                if dict_key:
                    result[dict_key] = dict_value
        return result

    def create_repo(self, repo):
        self.job.create_repo(repo)
