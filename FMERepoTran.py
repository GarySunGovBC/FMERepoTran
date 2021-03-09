import json
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

job = FMEServerJob(app_config, "source")
source_repo = job.list_repo_sub()
job = FMEServerJob(app_config, "dest")
dest_repo = job.list_repo_sub()
if debug:
    save_log(source_repo, "source")
    save_log(dest_repo, "dest")

