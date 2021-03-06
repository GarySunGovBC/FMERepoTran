import json
import sys

from FMERepositoryUtility.FMERepoCompare import FMERepositoryCompare
from FMERepositoryUtility.FMERepositoryCopy import FMERepositoryCopy
from FileLogger.Logger import AppLogger

CONFIG = "app.json"
SECRET_CONFIG = "secret.json"
JOB_CONFIG = "job.json"

# read app settings
with open(CONFIG) as app_config_json:
    app_config = json.load(app_config_json)
with open(SECRET_CONFIG) as secrect_config_json:
    secrect_config = json.load(secrect_config_json)
with open(JOB_CONFIG) as job_config_json:
    job_config = json.load(job_config_json)
log = AppLogger("output\\log.txt")


def create_job():
    if "copy" in sys.argv:
        return FMERepositoryCopy(app_config, secrect_config, job_config, log)
    if "compare" in sys.argv:
        return FMERepositoryCompare(app_config, secrect_config, job_config, log)


try:
    job = create_job()
    job.execute()
except Exception as e:
    log.write_line(e)
