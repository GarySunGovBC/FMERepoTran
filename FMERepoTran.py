import json
import os

from ApiException import APIException
from FMEServerJob import FMEServerJob

CONFIG = "app.json"
JOB_CONFIG = "job.json"

# read app settings
with open(CONFIG) as app_config_json:
    app_config = json.load(app_config_json)
with open(JOB_CONFIG) as job_config_json:
    job_config = json.load(job_config_json)
debug = app_config["run_mode"] == "debug"
try:
    src_job = FMEServerJob(app_config, "source")
    dest_job = FMEServerJob(app_config, "dest")
    source_repos = src_job.list_repos()
    for repo in source_repos:
        repo_name = repo["name"]
        if repo_name not in job_config["repo_filter"]:
            continue
        dest_job.create_repo(repo_name)
        fmw_list = src_job.list_repo_fmws(repo_name)
        fmw_dir = job_config["fmw_dir"]
        for fmw in fmw_list:
            fmw_name = fmw["name"]
            src_job.download_fmw(repo_name, fmw_name, fmw_dir, True)
            dest_job.upload_fmw(repo_name, fmw_name, fmw_dir, job_config["overwrite"])
except APIException as e:
    print(e.error["message"])
except Exception as e:
    print(e)
