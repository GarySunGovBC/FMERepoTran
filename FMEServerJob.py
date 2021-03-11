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

    def list_repos(self):
        repo_list = self.job.list_repos()
        return repo_list

    def get_fmw_props(self, api_method, repo_name, fmw_name):
        try:
            return api_method(repo_name, fmw_name)
        except APIException as e:
            return e.error

    def list_repo_fmws(self, repo_name, deep=False):
        fmw_list = self.job.list_repo_fmws(repo_name)
        if deep:
            for fmw in fmw_list:
                fmw["datasets"] = self.get_fmw_props(self.job.get_repo_datasets, repo_name, fmw["name"])
                fmw["parameters"] = self.get_fmw_props(self.job.list_repo_parameters, repo_name, fmw["name"])
                fmw["resources"] = self.get_fmw_props(self.job.list_repo_resources, repo_name, fmw["name"])
                fmw["services"] = self.get_fmw_props(self.job.list_repo_services, repo_name, fmw["name"])
        return fmw_list

    def create_repo(self, repo):
        self.job.create_repo(repo)

    def delete_repo(self, repo_name):
        self.job.delete_repo(repo_name)

    def upload_fmw(self, repo_name, fmw_name, file_path):
        self.job.upload_fmw(repo_name, fmw_name, file_path)

    def download_fmw(self, repo_name, fmw_name, file_path):
        self.job.download_fmw(repo_name, fmw_name, file_path)

    def delete_fmw(self, repo_name, fmw_name):
        self.job.delete_fmw(repo_name, fmw_name)
