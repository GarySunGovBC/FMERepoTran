import json
import os
from FMEAPI.CallAPIGet import CallAPIGET
from FMEAPI.CallAPIPost import CallAPIPOST
from FMEAPI.ApiException import APIException
from FMEAPI.CallAPIDelete import CallAPIDELETE


class FmeApis:
    CONFIG = "api.json"

    def __init__(self, server, token, log):
        self.server = server
        self.token = token
        self.log = log
        with open(FmeApis.CONFIG) as api_config_json:
            self.api_config = json.load(api_config_json)

    def create_api_caller(self, method="GET"):
        if method == "GET":
            return CallAPIGET(self.api_config, self.server, self.token, self.log)
        if method == "POST":
            return CallAPIPOST(self.api_config, self.server, self.token, self.log)
        if method == "DELETE":
            return CallAPIDELETE(self.api_config, self.server, self.token, self.log)

    def check_health(self):
        """check server if funning good, no token required"""
        return_obj = self.create_api_caller().call_api("health_check")
        if return_obj["text"]["status"] != "ok":
            raise Exception("FME Server is not healthy.")
        return True

    def list_repos(self):
        """
        Retrieves all repositories on the FME Server.
        :return:
        """
        return_obj = self.create_api_caller().call_api("list_repos")
        items = return_obj["text"]["items"]
        return items

    def get_repo_info(self, repo_name):
        """Retrieves information about a repository."""
        info = self.create_api_caller().call_api("get_repo_info", [repo_name])
        return info

    def list_repo_fmws(self, repo_name):
        """Retrieves a list of items in the repository. """
        return_obj = self.create_api_caller().call_api("list_repo_fmws", [repo_name])
        items = return_obj["text"]["items"]
        return items

    def get_repo_fmw(self, repo_name, fmw_name):
        """Retrieves information about a repository item."""
        item = self.create_api_caller().call_api("get_repo_fmw", [repo_name, fmw_name])
        result = item["text"]
        result["repositoryName"] = repo_name
        return result

    def get_fmw_datasets_info(self, repo_name, fmw_name, dataset_dir, dataset_name):
        """Retrieves information about a dataset associated with a repository item."""
        dataset_info = self.create_api_caller().call_api("get_fmw_datasets_info",
                                                         [repo_name, fmw_name, dataset_dir, dataset_name])
        return dataset_info

    def list_fmw_datasets_features(self, repo_name, fmw_name, dataset_dir, dataset_name):
        """Retrieves the feature types of a dataset associated with a repository item."""
        feature_list = self.create_api_caller().call_api("list_fmw_datasets_features",
                                                         [repo_name, fmw_name, dataset_dir, dataset_name])
        return feature_list

    def get_fmw_datasets_feature_info(self, repo_name, fmw_name, dataset_dir, dataset_name, feature_name):
        """Retrieves information about a feature type of a dataset that is associated with a repository item."""
        feature_info = self.create_api_caller().call_api("get_fmw_datasets_feature_info",
                                                         [repo_name, fmw_name, dataset_dir,
                                                          dataset_name, feature_name])
        return feature_info

    def get_fmw_datasets(self, repo_name, fmw_name):
        """Retrieves a list of datasets associated with a repository item."""
        src_dataset_list = self.create_api_caller().call_api("list_fmw_datasets", [repo_name, fmw_name, "source"])
        dest_dataset_list = self.create_api_caller().call_api("list_fmw_datasets",
                                                              [repo_name, fmw_name, "destination"])
        return {"source_datasets": src_dataset_list, "destination_datasets": dest_dataset_list}

    def list_fmw_parameters(self, repo_name, fmw_name):
        """Retrieves the published parameters of a workspace."""
        parameters_list = self.create_api_caller().call_api("list_fmw_parameters", [repo_name, fmw_name])
        return parameters_list

    def get_fmw_parameters_pub_info(self, repo_name, fmw_name, pub_name):
        """Retrieves a published parameter of a workspace."""
        pub_info = self.create_api_caller().call_api("get_fmw_parameters_pub_info", [repo_name, fmw_name, pub_name])
        return pub_info

    def list_fmw_resources(self, repo_name, fmw_name):
        """Returns a list of resources associated with to a repository item."""
        return_obj = self.create_api_caller().call_api("list_fmw_resources", [repo_name, fmw_name])
        items = return_obj["text"]
        return items

    def get_fmw_resources_info(self, repo_name, fmw_name, resource_name):
        """Retrieves information about a repository."""
        info = self.create_api_caller().call_api("get_fmw_resource", [repo_name, fmw_name, resource_name])
        return info

    def download_fmw_resource(self, repo_name, fmw_name, resource_name):
        """Downloads a repository item. """
        headers = {"Accept": "application/octet-stream"}
        response = self.create_api_caller().call_api("get_fmw_resource",
                                                     url_params=[repo_name, fmw_name, resource_name],
                                                     headers=headers)
        return response

    def upload_fmw_resource(self, repo_name, fmw_name, resource_name, file):
        """Uploads a single resource to a repository item. """
        if not os.path.exists(file):
            raise APIException("File not found: %s" % file)
        bin_file = {'file': open(file, 'rb')}
        headers = {"Content-Disposition": "attachment; filename=\"%s\"" % resource_name, "Accept": "application/json"}
        response = self.create_api_caller("POST").call_api_upload("create_fmw_resources", bin_file,
                                                                  [repo_name, fmw_name], [200], headers)
        return response

    def delete_fmw_resource(self, repo_name, fmw_name, resource_name):
        """Removes a resource from a repository item."""
        response = self.create_api_caller("DELETE").call_api("delete_fmw_resources",
                                                             [repo_name, fmw_name, resource_name])
        return response

    def list_fmw_services(self, repo_name, fmw_name):
        """Retrieves a list of FME Server services with which a repository item is registered."""
        return_obj = self.create_api_caller().call_api("list_fmw_services", [repo_name, fmw_name])
        items = return_obj["text"]
        return items

    def create_fmw_services(self, repo_name, fmw_name, services):
        """Registers FME Server services with a repository item."""
        headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
        response = self.create_api_caller("POST").call_api("create_fmw_service", [repo_name, fmw_name], [200], headers,
                                                           services)
        return response

    def create_repo(self, repo):
        """Adds a repository to the FME Server instance."""
        headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
        response = self.create_api_caller("POST").call_api("create_repo", None, None, headers=headers, body=repo)
        return response

    def delete_repo(self, repo_name):
        """Removes a repository and all of its contents."""
        response = self.create_api_caller("DELETE").call_api("delete_repo", [repo_name])
        return response

    def download_fmw(self, repo_name, fmw_name):
        """Downloads a repository item. """
        headers = {"Accept": "application/octet-stream"}
        response = self.create_api_caller().call_api("get_repo_fmw", url_params=[repo_name, fmw_name], headers=headers)
        return response

    def upload_fmw(self, repo_name, fmw_name, file):
        """Uploads an item to a repository. """
        if not os.path.exists(file):
            raise APIException("File not found: %s" % file)
        bin_file = {'file': open(file, 'rb')}
        headers = {"Content-Disposition": "attachment; filename=\"%s\"" % fmw_name, "Accept": "application/json"}
        response = self.create_api_caller("POST").call_api_upload("create_fmw", bin_file, [repo_name], [200, 201],
                                                                  headers)
        return response

    def delete_fmw(self, repo_name, fmw_name):
        """Removes an item from a repository."""
        response = self.create_api_caller("DELETE").call_api("delete_fmw", [repo_name, fmw_name], [204])
        return response
