import json
import os

from ApiException import APIException
from FMEServerJob import FMEServerJob

CONFIG = "app.json"


def save_log(repos, key):
    f = open(key + ".repo.txt", "w")
    for repo in repos:
        f.write(repo + "\n")
        for sub in repos[repo]:
            f.write("\t" + sub + "\n")
    f.close()


# read app settings
with open(CONFIG) as app_config_json:
    app_config = json.load(app_config_json)
debug = app_config["run_mode"] == "debug"
try:
    # job = FMEServerJob(app_config, "source")
    # source_repos = job.list_repo_fmw()
    job = FMEServerJob(app_config, "dest")
    # fmw_list = job.list_repo_fmws("GARY_TEST_REPO")
    # for fmw in fmw_list:
    #     job.delete_fmw("GARY_TEST_REPO", fmw["name"])
    # for fn in os.listdir("FMWs"):
    #     job.upload_fmw("GARY_TEST_REPO", fn, "FMWs")
    fmw_list = job.list_repo_fmws("GARY_TEST_REPO")
    for fmw in fmw_list:
        job.download_fmw("GARY_TEST_REPO", fmw["name"],"download")
    # for fmw in fmw_list:
    #     job.delete_fmw("GARY_TEST_REPO", fmw["name"])
    #     break
    # # job.delete_repo("GaryTest")
    # if debug:
    #     pass
    # save_log(source_repo, "source")
    # save_log(dest_repo, "dest")
except APIException as e:
    print(e.error["message"])
