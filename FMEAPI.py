import os

from ApiException import APIException
from CallAPIDelete import CallAPIDELETE
from CallAPIGet import CallAPIGET
from CallAPIPost import CallAPIPOST


class FmeApis(object):

    def __init__(self, app_config, server, token):
        self.app_config = app_config
        self.server = server
        self.token = token

    def create_api_caller(self, method="GET"):
        if method == "GET":
            return CallAPIGET(self.app_config, self.server, self.token)
        if method == "POST":
            return CallAPIPOST(self.app_config, self.server, self.token)
        if method == "DELETE":
            return CallAPIDELETE(self.app_config, self.server, self.token)

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
        return item["text"]

    def get_repo_datasets_info(self, repo_name, fmw_name, dataset_dir, dataset_name):
        """Retrieves information about a dataset associated with a repository item."""
        dataset_info = self.create_api_caller().call_api("get_repo_datasets_info",
                                                         [repo_name, fmw_name, dataset_dir, dataset_name])
        return dataset_info

    def list_repo_datasets_features(self, repo_name, fmw_name, dataset_dir, dataset_name):
        """Retrieves the feature types of a dataset associated with a repository item."""
        feature_list = self.create_api_caller().call_api("list_repo_datasets_features",
                                                         [repo_name, fmw_name, dataset_dir, dataset_name])
        return feature_list

    def get_repo_datasets_feature_info(self, repo_name, fmw_name, dataset_dir, dataset_name, feature_name):
        """Retrieves information about a feature type of a dataset that is associated with a repository item."""
        feature_info = self.create_api_caller().call_api("get_repo_datasets_feature_info",
                                                         [repo_name, fmw_name, dataset_dir,
                                                          dataset_name, feature_name])
        return feature_info

    def get_repo_datasets(self, repo_name, fmw_name):
        """Retrieves a list of datasets associated with a repository item."""
        src_dataset_list = self.create_api_caller().call_api("list_repo_datasets", [repo_name, fmw_name, "source"])
        dest_dataset_list = self.create_api_caller().call_api("list_repo_datasets",
                                                              [repo_name, fmw_name, "destination"])
        return {"source_datasets": src_dataset_list, "destination_datasets": dest_dataset_list}

    def list_repo_parameters(self, repo_name, fmw_name):
        """Retrieves the published parameters of a workspace."""
        parameters_list = self.create_api_caller().call_api("list_repo_parameters", [repo_name, fmw_name])
        return parameters_list

    def get_repo_parameters_pub_info(self, repo_name, fmw_name, pub_name):
        """Retrieves a published parameter of a workspace."""
        pub_info = self.create_api_caller().call_api("get_repo_parameters_pub_info", [repo_name, fmw_name, pub_name])
        return pub_info

    def list_repo_resources(self, repo_name, fmw_name):
        """Returns a list of resources associated with to a repository item."""
        list_resources = self.create_api_caller().call_api("list_repo_resources", [repo_name, fmw_name])
        return list_resources

    def get_repo_resources_info(self, repo_name, fmw_name, resource_name):
        """Downloads a repository item resource."""
        resource_info = self.create_api_caller().call_api("get_repo_resources_info",
                                                          [repo_name, fmw_name, resource_name])
        return resource_info

    def list_repo_services(self, repo_name, fmw_name):
        """Retrieves a list of FME Server services with which a repository item is registered."""
        list_services = self.create_api_caller().call_api("list_repo_services", [repo_name, fmw_name])
        return list_services

    def create_repo(self, repo):
        """Adds a repository to the FME Server instance."""
        response = self.create_api_caller("POST").call_api("create_repo", None, repo)
        return response

    def delete_repo(self, repo_name):
        """Removes a repository and all of its contents."""
        response = self.create_api_caller("DELETE").call_api("delete_repo", [repo_name])
        return response

    def download_fmw(self, repo_name, fmw_name, file_path, overwrite=False):
        """Downloads a repository item. """
        fmw_file = os.path.join(file_path, fmw_name)
        if not overwrite and os.path.exists(fmw_file):
            raise APIException("File already exists: %s" % fmw_file)
        headers = {"Accept": "application/octet-stream"}
        response = self.create_api_caller().call_api("get_repo_fmw", [repo_name, fmw_name], None, headers)
        with open(fmw_file, 'wb') as f:
            f.write(response["response"].content)
        return response

    def upload_fmw(self, repo_name, fmw_name, file_path):
        """Uploads an item to a repository. """
        fmw_file = os.path.join(file_path, fmw_name)
        if not os.path.exists(fmw_file):
            raise APIException("File not found: %s" % fmw_file)
        files = {'file': open(fmw_file, 'rb')}
        headers = {"Content-Disposition": "attachment; filename=\"%s\"" % fmw_name, "Accept": "application/json"}
        response = self.create_api_caller("POST").call_api_upload("create_fmw", files, [repo_name], [200, 201], headers)
        return response

    def delete_fmw(self, repo_name, fmw_name):
        """Removes an item from a repository."""
        response = self.create_api_caller("DELETE").call_api("delete_fmw", [repo_name, fmw_name], [204])
        return response
