import json
import os
from datetime import datetime

from ApiException import APIException
from FMEServerJob import FMEServerJob

CONFIG = "app.json"
JOB_CONFIG = "job.json"
SECRET_CONFIG = "secret.json"


def log(obj):
    f = open("log.txt", "a")
    f.write("%s" % obj)
    f.write("\n")
    f.close()


# read app settings
with open(CONFIG) as app_config_json:
    app_config = json.load(app_config_json)
with open(JOB_CONFIG) as job_config_json:
    job_config = json.load(job_config_json)
with open(SECRET_CONFIG) as secrect_config_json:
    secrect_config = json.load(secrect_config_json)
debug = app_config["run_mode"] == "debug"
try:
    # raise Exception()
    log({"start at": datetime.now()})
    src_job = FMEServerJob(app_config, secrect_config, "source")
    dest_job = FMEServerJob(app_config, secrect_config, "dest")
    source_repos = src_job.list_repos()
    for repo in source_repos:
        repo_name = repo["name"]
        if repo_name not in job_config["repo_filter"].keys():
            continue
        if job_config["repo_filter"][repo_name] != "1":
            continue

        # if repo_name != "BCGW_REP_ON_REQUEST":
        #     continue
        #
        # print('Transfer Repo "%s". Continue(y/n)?' % repo_name)
        # x = input()
        # if x != 'y':
        #     continue
        dest_job.create_repo(repo)
        fmw_list = src_job.list_repo_fmws(repo_name)
        fmw_dir = job_config["fmw_dir"]
        for fmw in fmw_list:
            fmw_name = fmw["name"]
            full_name = "%s\%s" % (repo_name, fmw_name)
            print("Transfering %s ..." % full_name)
            if job_config["fmw_filter"]["on"]:
                if full_name not in job_config["fmw_filter"]["items"]:
                    continue
            src_job.download_fmw(repo_name, fmw_name, fmw_dir, True)
            try:
                dest_job.upload_fmw(repo_name, fmw_name, fmw_dir, job_config["overwrite"])
            except APIException as e:
                log(e.error["message"])
except APIException as e:
    print(e.error["message"])
    log(e.error["message"])
except Exception as e:
    print(e)
    log(e)
