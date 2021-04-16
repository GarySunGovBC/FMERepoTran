import json
import sys

import FileLogger
from FMERepositoryUtility.FMERepoCompare import FMERepositoryCompare
from FMERepositoryUtility.FMERepositoryCopy import FMERepositoryCopy
from FileLogger.Logger import AppLogger

CONFIG = "app.json"
SECRET_CONFIG = "secret.json"
JOB_CONFIG = "test.json"

# read app settings
with open(CONFIG) as app_config_json:
    app_config = json.load(app_config_json)
with open(SECRET_CONFIG) as secrect_config_json:
    secrect_config = json.load(secrect_config_json)
with open(JOB_CONFIG) as job_config_json:
    job_config = json.load(job_config_json)

try:
    test_log = AppLogger("output\\test_log.txt")
    # test_log.test()
    test_copy = FMERepositoryCopy(app_config, secrect_config, job_config, test_log)
    test_compare = FMERepositoryCompare(app_config, secrect_config, job_config, test_log)
    #test_copy.execute()
    test_compare.execute()
    print("Passed!")
except Exception as e:
    print(e)
    print("Failed!")
