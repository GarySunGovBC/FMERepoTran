from ApiException import APIException
from FMEServerJob import FMEServerJob
from FMWCompare import FMWCompare
from RepoCompare import RepoCompare


class FMERepositoryCompare(FMEServerJob):

    def do_fmw_job(self, repo, fmw):
        repo_name = repo["name"]
        fmw_name = fmw["name"]
        full_name = "%s\%s" % (repo_name, fmw_name)
        if self.job_config["fmw_filter"]["on"]:
            if full_name not in self.job_config["fmw_filter"]["items"]:
                return
        self.log.write_line("Comaring %s ..." % full_name)
        if not self.dest_job.fmw_exists(repo_name, fmw_name):
            raise APIException("FMW missing in destination: %s\%s." % (repo_name, fmw_name))
        src_fmw = self.src_job.get_repo_fmw(repo_name, fmw_name)
        dest_fmw = self.dest_job.get_repo_fmw(repo_name, fmw_name)
        fmw_compare = FMWCompare(src_fmw, dest_fmw)
        try:
            fmw_compare.compare()
        except APIException as e:
            raise APIException("fmw not equal: %s. reason: %s" % (full_name, e.error["message"]))

    def do_repo_job(self, src_repo, dest_repos):
        repo_name = src_repo["name"]
        try:
            if not dest_repos:
                raise APIException("Repository missing in destination: %s." % repo_name)
            repo_compare = RepoCompare(src_repo, dest_repos)
            repo_compare.compare()
            return True
        except APIException as e:
            self.log.write_line("Repository not equal: %s. reason: %s" % (repo_name, e.error["message"]))
            return False
