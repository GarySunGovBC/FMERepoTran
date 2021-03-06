import json
import FMEAPI

CONFIG = "app.json"

# read app settings
with open(CONFIG) as app_config_json:
    app_config = json.load(app_config_json)

job = FMEAPI.FmeApis(app_config, "source_fme_server", app_config["source_token"])
ok = job.check_health()
if not ok:
    raise Exception("FME Server is not healthy.")
repo_list = job.list_repos()
for repo in repo_list:
    print(repo["name"])
    sub_list = job.list_repo_sub_items(repo["name"])
    for sub in sub_list:
        print(sub["name"])
input()
