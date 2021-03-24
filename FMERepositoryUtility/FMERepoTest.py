from FMERepositoryUtility.FMEServerJob import FMEServerJob
from FMERepositoryUtility.RepoCompare import RepoCompare


class FMERepoTest(FMEServerJob):
    def __init__(self, app_config, secrect_config, job_config, log, key):
        super.__init__(app_config, secrect_config, job_config)
        self.key = key

    def execute(self):
        for repo in self.job_config:
            repo_compare = RepoCompare(src_repo, dest_repos)
        if self.key == "compare":
            repo_compare.compare()
