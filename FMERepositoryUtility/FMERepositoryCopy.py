from FMEAPI.ApiException import APIException
from FMERepositoryUtility.FMEServerJob import FMEServerJob


class FMERepositoryCopy(FMEServerJob):

    def copy_resource(self, src_repo_name, dest_repo_name, fmw_name, resource):
        resource_name = resource["name"]
        try:
            n = int(resource["size"])
            # download and upload file size has limit. Too big will hang the program
            if n > self.max_file_size:
                raise APIException("Skipped resource: %s, size: [{:,}].".format(n) % resource_name)
            self.log.write_line(
                "Transfering resource %s, size: [{:,}] ...".format(n) % resource_name)
            self.src_job.download_fmw_resource(src_repo_name, fmw_name, resource_name,
                                               self.resource_dir, True)
            self.dest_job.upload_fmw_resource(src_repo_name, dest_repo_name, fmw_name, resource_name,
                                              self.resource_dir, self.overwrite_fmw)
        except ValueError:
            raise APIException("Skipped resource: %s" % resource_name)

    def do_fmw_job(self, repo, dest_repo_name, fmw):
        src_repo_name = repo["name"]
        fmw_name = fmw["name"]
        full_name = "%s\%s" % (src_repo_name, fmw_name)
        if self.job_config["fmw_filter"]["on"]:
            if full_name not in self.job_config["fmw_filter"]["items"]:
                return
        self.log.write_line("Transfering %s ..." % full_name)
        self.src_job.download_fmw(src_repo_name, fmw_name, self.fmw_dir, True)
        self.dest_job.upload_fmw(src_repo_name, dest_repo_name, fmw_name, self.fmw_dir, self.overwrite_fmw)
        services = self.src_job.list_fmw_services(src_repo_name, fmw_name)
        if len(services) > 0:
            for svc in services:
                self.log.write_line("Transfering service %s ..." % svc["name"])
            self.dest_job.create_fmw_services(dest_repo_name, fmw_name, services)
        resources = self.src_job.list_fmw_resources(src_repo_name, fmw_name)
        for resource in resources:
            self.copy_resource(src_repo_name, src_repo_name, fmw_name, resource)

    def do_repo_job(self, repo, dest_repo_name, dest_repo):
        """copy """
        src_repo_name = repo["name"]
        repo["name"] = dest_repo_name
        try:
            self.dest_job.create_repo(repo)
            return True
        finally:
            repo["name"] = src_repo_name
