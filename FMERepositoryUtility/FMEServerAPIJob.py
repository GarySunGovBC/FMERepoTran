import os

from FMEAPI import FMEAPI
from FMEAPI.ApiException import APIException
from pathlib import Path


class FMEServerAPIJob:

    def __init__(self, job_config, secrect_config, key, output_dir, log):
        self.job_config = job_config
        self.key = key
        self.job = FMEAPI.FmeApis(job_config[key + "_fme_server"], secrect_config[key + "_token"], log)
        self.output_dir = output_dir
        ok = self.job.check_health()
        if not ok:
            raise Exception("FME Server is not healthy.")

    def list_repos(self):
        repo_list = self.job.list_repos()
        return repo_list

    def get_fmw_props(self, api_method, repo_name, fmw_name):
        try:
            return api_method(repo_name, fmw_name)
        except APIException as e:
            return e.error

    def list_repo_fmws(self, repo_name, deep=False):
        fmw_list = self.job.list_repo_fmws(repo_name)
        if deep:
            for fmw in fmw_list:
                fmw["datasets"] = self.get_fmw_props(self.job.get_fmw_datasets, repo_name, fmw["name"])
                fmw["parameters"] = self.get_fmw_props(self.job.list_fmw_parameters, repo_name, fmw["name"])
                fmw["resources"] = self.get_fmw_props(self.job.list_fmw_resources, repo_name, fmw["name"])
                fmw["services"] = self.get_fmw_props(self.job.list_fmw_services, repo_name, fmw["name"])
        return fmw_list

    def create_repo(self, repo):
        repo_name = repo["name"]
        if self.repo_exists(repo_name):
            if not self.job_config["overwrite_repo"]:
                return
            else:
                print("Do you want to delete the repository: %s?(y/n)" % repo_name)
                enter_key = input()
                if enter_key == "y" or enter_key == "Y":
                    self.job.delete_repo(repo_name)
                else:
                    return
        self.job.create_repo(repo)

    def prop_exists(self, names, get_prop_act):
        try:
            get_prop_act(*names)
            return True
        except APIException:
            return False

    def repo_exists(self, repo_name):
        get_prop_func = lambda repo_name: self.job.get_repo_info(repo_name)
        return self.prop_exists([repo_name], get_prop_func)

    def delete_repo(self, repo_name):
        self.job.delete_repo(repo_name)

    def delete_fmw(self, repo_name, fmw_name):
        self.job.delete_fmw(repo_name, fmw_name)

    def fmw_exists(self, repo_name, fmw_name):
        get_prop_func = lambda repo_name, fmw_name: self.job.get_repo_fmw(repo_name, fmw_name)
        return self.prop_exists([repo_name, fmw_name], get_prop_func)

    def get_repo_fmw(self, repo_name, fmw_name):
        fmw = self.job.get_repo_fmw(repo_name, fmw_name)
        return fmw

    def download_fmw(self, repo_name, fmw_name, dir, overwrite=False):
        download_prop_func = lambda repo_name, fmw_name: self.job.download_fmw(repo_name, fmw_name)
        return self.download_prop([repo_name, fmw_name], dir, overwrite, download_prop_func)

    def delete_fmw(self, repo_name, fmw_name):
        if self.fmw_exists(repo_name, fmw_name):
            self.job.delete_fmw(repo_name, fmw_name)

    def download_repo(self, repo_name, dir):
        fmw_dir = os.path.join(dir, repo_name)
        if not os.path.exists(fmw_dir):
            os.makedirs(fmw_dir)
        fmw_list = self.list_repo_fmws(repo_name)
        for fmw in fmw_list:
            self.download_fmw(repo_name, fmw["name"], fmw_dir)

    def populate_prop_file_name(self, names, dir):
        if len(names) == 0:
            return None
        if len(names) == 1:
            return os.path.join(self.output_dir, dir, names[0])
        if len(names) == 2:
            return os.path.join(self.output_dir, dir, names[0], names[1])
        return os.path.join(self.output_dir, dir, names[0], names[1], names[2])

    def download_prop(self, names, dir, overwrite, download_prop_act):
        prop_file = self.populate_prop_file_name(names, dir)
        if not overwrite and os.path.exists(prop_file):
            raise APIException("File already exists: %s" % prop_file)
        response = download_prop_act(*names)
        Path(os.path.dirname(prop_file)).mkdir(parents=True, exist_ok=True)
        with open(prop_file, 'wb') as f:
            f.write(response["response"].content)
        return response

    def upload_fmw_prop(self, names, dir, overwrite, delete_act, exist_act, upload_act):
        # remove src to get dest names
        # remove dest to get src names
        if len(names) > 2:
            src_names = names[0:1]
            src_names.extend(names[2:len(names)])
            dest_names = names[1:len(names)]
        prop_file = self.populate_prop_file_name(src_names, dir)
        if not os.path.exists(prop_file):
            raise Exception("File not found: %s" % prop_file)
        error = "Failed to upload %s." % prop_file
        if overwrite:
            try:
                delete_act(*dest_names)
            except:
                pass
            if exist_act(*dest_names):
                raise APIException(error)
        else:
            if exist_act(*dest_names):
                return
        response = upload_act(*dest_names, prop_file)
        if not exist_act(*dest_names):
            raise APIException(error)
        return response

    def upload_fmw(self, src_repo_name, dest_repo_name, fmw_name, dir, overwrite=False):
        del_func = lambda dest_repo_name, fmw_name: self.job.delete_fmw(dest_repo_name, fmw_name)
        exist_func = lambda dest_repo_name, fmw_name: self.fmw_exists(dest_repo_name, fmw_name)
        upload_func = lambda repo_name, fmw_name, file: self.job.upload_fmw(dest_repo_name, fmw_name,
                                                                            file)
        return self.upload_fmw_prop([src_repo_name, dest_repo_name, fmw_name], dir, overwrite, del_func, exist_func,
                                    upload_func)

    def list_fmw_services(self, repo_name, fmw_name):
        return self.job.list_fmw_services(repo_name, fmw_name)

    def create_fmw_services(self, repo_name, fmw_name, services):
        text = ""
        for service in services:
            text = "%s&services=%s" % (text, service["name"])
        text = text.lstrip("&")
        self.job.create_fmw_services(repo_name, fmw_name, text)

    def list_fmw_resources(self, repo_name, fmw_name):
        return self.job.list_fmw_resources(repo_name, fmw_name)

    def download_fmw_resource(self, repo_name, fmw_name, resource_name, dir, overwrite=False):
        download_prop_func = lambda repo_name, fmw_name, resource_name: self.job.download_fmw_resource(repo_name,
                                                                                                       fmw_name,
                                                                                                       resource_name)
        return self.download_prop([repo_name, fmw_name, resource_name], dir, overwrite, download_prop_func)

    def upload_fmw_resource(self, src_repo_name, dest_repo_name, fmw_name, resource_name, dir, overwrite=False):
        del_func = lambda dest_repo_name, fmw_name, resource_name: self.job.delete_fmw_resource(dest_repo_name,
                                                                                                fmw_name,
                                                                                                resource_name)
        exist_func = lambda dest_repo_name, fmw_name, resource_name: self.fmw_resource_exists(dest_repo_name, fmw_name,
                                                                                              resource_name)
        upload_func = lambda dest_repo_name, fmw_name, resource_name, file: self.job.upload_fmw_resource(
            dest_repo_name, fmw_name,
            resource_name, file)
        return self.upload_fmw_prop([src_repo_name, dest_repo_name, fmw_name, resource_name], dir, overwrite, del_func,
                                    exist_func,
                                    upload_func)

    def delete_fmw_resources(self, repo_name, fmw_name, resource_name):
        self.job.delete_fmw_resources(repo_name, fmw_name, resource_name)

    def fmw_resource_exists(self, repo_name, fmw_name, resource_name):
        get_prop_func = lambda repo_name, fmw_name, resource_name: self.job.get_fmw_resources_info(repo_name, fmw_name,
                                                                                                   resource_name)
        return self.prop_exists([repo_name, fmw_name, resource_name], get_prop_func)
