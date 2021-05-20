import json
import os
import sys

from FMERepoCompare import FMERepositoryCompare
from FMERepositoryCopy import FMERepositoryCopy

sys.path.append('../FMEServerLib')

from FileLogger.Logger import AppLogger

CONFIG = "app.json"
SECRET_CONFIG = "secret.json"
JOB_CONFIG = "job.json"

# read app settings
with open(CONFIG) as app_config_json:
    app_config = json.load(app_config_json)
log = AppLogger(os.path.join(app_config["log_dir"], "log.txt"), True, True)


def create_job():
    if "copy" in sys.argv:
        return FMERepositoryCopy(CONFIG, SECRET_CONFIG, JOB_CONFIG)
    if "compare" in sys.argv:
        return FMERepositoryCompare(CONFIG, SECRET_CONFIG, JOB_CONFIG)


try:
    job = create_job()
    job.execute()
except Exception as e:
    log.write_line(e)
