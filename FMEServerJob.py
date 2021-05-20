import json
import os
from datetime import datetime
from FMEAPI.ApiException import APIException
from FMERepositoryUtility.FMEServerAPIJob import FMEServerAPIJob
from FileLogger.Logger import AppLogger


class FMEServerJob:

    def __init__(self, app_config_name, secret_config_name, job_config_name):
        self.secret_config_name = secret_config_name
        self.job_config_name = job_config_name
        with open(app_config_name) as app_config_json:
            self.app_config = json.load(app_config_json)
        with open(secret_config_name) as secret_config_json:
            self.secret_config = json.load(secret_config_json)
        with open(job_config_name) as job_config_json:
            self.job_config = json.load(job_config_json)
        self.debug = self.app_config["run_mode"] == "debug"
        self.log = AppLogger(os.path.join(self.app_config["log_dir"], "log.txt"), True, True)
        self.fmw_dir = self.job_config["fmw_dir"]
        self.max_file_size = self.job_config["max_file_size"]
        self.resource_dir = self.job_config["resource_dir"]
        self.src_job = FMEServerAPIJob(secret_config_name, job_config_name, self.job_config["source_fme_server"],
                                       self.secret_config["source_token"])
        self.dest_job = FMEServerAPIJob(secret_config_name, job_config_name, self.job_config["dest_fme_server"],
                                        self.secret_config["dest_token"])
        self.overwrite_repo = self.job_config["overwrite_repo"]
        self.overwrite_fmw = self.job_config["overwrite_fmw"]

    def do_repo_job(self, src_repo, dest_repo_name, dest_repo):
        pass

    def do_fmw_job(self, repo, dest_repo_name, fmw):
        pass

    def execute(self):
        self.log.write_line({"start at": datetime.now()})
        source_repos = self.src_job.list_repos()
        dest_repos = self.dest_job.list_repos()
        for repo in source_repos:
            src_repo_name = repo["name"]
            if src_repo_name not in self.job_config["repo_filter"].keys():
                continue
            if self.job_config["repo_filter"][src_repo_name] != "1":
                continue
            dest_repo_name = src_repo_name
            for repo_replace in self.job_config["repo_replace"]:
                if dest_repo_name in repo_replace.keys():
                    dest_repo_name = repo_replace[dest_repo_name]
            dest_repo = next((r for r in dest_repos if r["name"] == dest_repo_name), None)
            if not self.do_repo_job(repo, dest_repo_name, dest_repo):
                continue
            fmw_list = self.src_job.list_repo_fmws(src_repo_name)
            for fmw in fmw_list:
                try:
                    self.do_fmw_job(repo, dest_repo_name, fmw)
                except APIException as e:
                    self.log.write_line(e.error["message"])
