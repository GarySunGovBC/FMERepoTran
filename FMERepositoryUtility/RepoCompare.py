import json
from FMERepositoryUtility.ObjectCompare import ObjectCompare


class RepoCompare:
    CONFIG = "repo.json"

    def __init__(self, repo1, repo2):
        self.repo1 = repo1
        self.repo2 = repo2
        with open(RepoCompare.CONFIG) as repo_config_json:
            self.repo_config = json.load(repo_config_json)

    def compare(self):
        """ compare 2 repo """
        ObjectCompare.object_equal([self.repo1, self.repo2], self.repo_config["repo"]["props"])
