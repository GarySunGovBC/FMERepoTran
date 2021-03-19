from datetime import datetime
from ApiException import APIException
from FMEServerJob import FMEServerJob


class FMERepositoryCopy(FMEServerJob):

    def copy_resource(self, repo_name, fmw_name, resource):
        resource_name = resource["name"]
        try:
            n = int(resource["size"])
            # download and upload file size has limit. Too big will hang the program
            if n > self.max_file_size:
                raise APIException("Skipped resource: %s, size: [{:,}].".format(n) % resource_name)
            self.log.write_line(
                "Transfering resource %s, size: [{:,}] ...".format(n) % resource_name)
            self.src_job.download_fmw_resource(repo_name, fmw_name, resource_name,
                                               self.resource_dir, True)
            self.dest_job.upload_fmw_resource(repo_name, fmw_name, resource_name,
                                              self.resource_dir, self.overwrite)
        except ValueError:
            raise APIException("Skipped resource: %s" % resource_name)

    def do_fmw_job(self, repo, fmw):
        repo_name = repo["name"]
        fmw_name = fmw["name"]
        full_name = "%s\%s" % (repo_name, fmw_name)
        if self.job_config["fmw_filter"]["on"]:
            if full_name not in self.job_config["fmw_filter"]["items"]:
                return
        self.log.write_line("Transfering %s ..." % full_name)
        self.src_job.download_fmw(repo_name, fmw_name, self.fmw_dir, True)
        self.dest_job.upload_fmw(repo_name, fmw_name, self.fmw_dir, self.overwrite)
        services = self.src_job.list_fmw_services(repo_name, fmw_name)
        if len(services) > 0:
            for svc in services:
                self.log.write_line("Transfering service %s ..." % svc["name"])
            self.dest_job.create_fmw_services(repo_name, fmw_name, services)
        resources = self.src_job.list_fmw_resources(repo_name, fmw_name)
        for resource in resources:
            self.copy_resource(repo_name, fmw_name, resource)

    def do_repo_job(self, repo):
        self.dest_job.create_repo(repo)
        repo_name = repo["name"]
        fmw_list = self.src_job.list_repo_fmws(repo_name)
        for fmw in fmw_list:
            self.copy_fmw(repo_name, fmw)
