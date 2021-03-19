import json
from datetime import datetime

from ApiException import APIException
from FMEServerAPIJob import FMEServerAPIJob

JOB_CONFIG = "job.json"


class FMEServerJob:

    def __init__(self, app_config, secrect_config, log):
        self.debug = app_config["run_mode"] == "debug"
        self.overwrite = app_config["overwrite"]
        self.log = log
        with open(JOB_CONFIG) as job_config_json:
            self.job_config = json.load(job_config_json)
        self.fmw_dir = self.job_config["fmw_dir"]
        self.max_file_size = self.job_config["max_file_size"]
        self.resource_dir = self.job_config["resource_dir"]
        self.src_job = FMEServerAPIJob(app_config, secrect_config, "source", self.job_config["output_dir"], log)
        self.dest_job = FMEServerAPIJob(app_config, secrect_config, "dest", self.job_config["output_dir"], log)

    def do_repo_job(self, repo):
        pass

    def do_fmw_job(self, repo, fmw):
        pass

    def execute(self):
        try:
            self.log.write_line({"start at": datetime.now()})
            source_repos = self.src_job.list_repos()
            for repo in source_repos:
                repo_name = repo["name"]
                if repo_name not in self.job_config["repo_filter"].keys():
                    continue
                if self.job_config["repo_filter"][repo_name] != "1":
                    continue
                self.do_repo_job(repo)
                fmw_list = self.src_job.list_repo_fmws(repo_name)
                for fmw in fmw_list:
                    try:
                        self.do_fmw_job(repo, fmw)
                    except APIException as e:
                        self.log.write_line(e.error["message"])
        except APIException as e:
            self.log.write_line(e.error["message"])
