import json
import os

import FMEAPI
from ApiException import APIException


class FMEServerJob(object):

    def __init__(self, app_config, secrect_config, key):
        self.key = key
        self.app_config = app_config
        self.job = FMEAPI.FmeApis(app_config, key + "_fme_server", secrect_config[key + "_token"])
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
        if self.repo_exists(repo["name"]):
            return
        self.job.create_repo(repo)

    def repo_exists(self, repo_name):
        try:
            self.job.get_repo_info(repo_name)
            return True
        except APIException:
            return False

    def delete_repo(self, repo_name):
        self.job.delete_repo(repo_name)

    def delete_fmw(self, repo_name, fmw_name):
        self.job.delete_fmw(repo_name, fmw_name)

    def fmw_exists(self, repo_name, fmw_name):
        try:
            self.job.get_repo_fmw(repo_name, fmw_name)
            return True
        except APIException:
            return False

    def get_repo_fmw(self, repo_name, fmw_name):
        fmw = self.job.get_repo_fmw(repo_name, fmw_name)
        return fmw

    def download_fmw(self, repo_name, fmw_name, file_path, overwrite=False):
        self.job.download_fmw(repo_name, fmw_name, file_path, overwrite)

    def delete_fmw(self, repo_name, fmw_name):
        if self.fmw_exists(repo_name, fmw_name):
            self.job.delete_fmw(repo_name, fmw_name)

    def download_repo(self, repo_name, dir):
        fmw_dir = os.path.join(dir, repo_name)
        if not os.path.exists(fmw_dir):
            os.makedirs(fmw_dir)
        fmw_list = self.list_repo_fmws(repo_name)
        for fmw in fmw_list:
            self.download_fmw(repo_name, fmw["name"], fmw_dir)

    def upload_fmw(self, repo_name, fmw_name, dir, overwrite=False):
        fmw_dir = os.path.join(dir, repo_name)
        if not os.path.exists(fmw_dir):
            raise Exception("File not found: %s" % fmw_name)
        if overwrite:
            try:
                self.job.delete_fmw(repo_name, fmw_name)
            except:
                pass
            if self.fmw_exists(repo_name, fmw_name):
                raise APIException("Failed to transfer %s\%s" % (repo_name, fmw_name))
        else:
            if self.fmw_exists(repo_name, fmw_name):
                return
        self.job.upload_fmw(repo_name, fmw_name, dir)
        if not self.fmw_exists(repo_name, fmw_name):
            raise APIException("Failed to transfer %s\%s" % (repo_name, fmw_name))
