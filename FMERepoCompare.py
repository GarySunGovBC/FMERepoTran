import json
import os
from datetime import datetime

from ApiException import APIException
from FMEServerJob import FMEServerJob
from FMWCompare import FMWCompare

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
        # print('Transfer Repo "%s". Continue(y/n)?' % repo_name)
        # x = input()
        # if x != 'y':
        #     continue
        if not dest_job.repo_exists(repo_name):
            log("Repository missing in destination: %s." % repo_name)
            continue
        fmw_list = src_job.list_repo_fmws(repo_name)
        for fmw in fmw_list:
            fmw_name = fmw["name"]
            full_name = "%s\%s" % (repo_name, fmw_name)
            print("Comaring %s ..." % full_name)
            if job_config["fmw_filter"]["on"]:
                if full_name not in job_config["fmw_filter"]["items"]:
                    continue
            if not dest_job.fmw_exists(repo_name, fmw_name):
                log("FMW missing in destination: %s\%s." % (repo_name, fmw_name))
                continue
            src_fmw = src_job.get_repo_fmw(repo_name, fmw_name)
            dest_fmw = dest_job.get_repo_fmw(repo_name, fmw_name)
            fmw_compare = FMWCompare(src_fmw, dest_fmw)
            try:
                fmw_compare.compare()
            except Exception as e:
                log("fmw not equal: %s\%s. reason: %s" % (repo_name, fmw_name, e.error["message"]))
except APIException as e:
    print(e.error["message"])
    log(e.error["message"])
except Exception as e:
    print(e)
    log(e)
