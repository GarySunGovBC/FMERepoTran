import json

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
    # source_repo = job.list_repo_sub()
    job = FMEServerJob(app_config, "dest")
    #dest_repo = job.list_repo()
    repo = {'owner': 'admin',
            'name': 'GARY_TEST_REPO_4',
            'description': 'This repository is for Gary\'s tests.',
            'sharable': True
            }
    job.create_repo(repo)
    if debug:
        pass
        # save_log(source_repo, "source")
        # save_log(dest_repo, "dest")
except APIException as e:
    print(e.error["message"])
