from datetime import datetime
from FMEAPI.ApiException import APIException
from FMERepositoryUtility.FMEServerAPIJob import FMEServerAPIJob


class FMEServerJob:

    def __init__(self, app_config, secrect_config, job_config, log):
        self.debug = app_config["run_mode"] == "debug"
        self.job_config = job_config
        self.log = log
        self.fmw_dir = self.job_config["fmw_dir"]
        self.max_file_size = self.job_config["max_file_size"]
        self.resource_dir = self.job_config["resource_dir"]
        self.src_job = FMEServerAPIJob(job_config, secrect_config, "source", self.job_config["output_dir"], log)
        self.dest_job = FMEServerAPIJob(job_config, secrect_config, "dest", self.job_config["output_dir"], log)
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
