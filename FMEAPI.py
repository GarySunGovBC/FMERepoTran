import CallAPI


class FmeApis(object):

    def __init__(self, app_config, server, token):
        self.api_call_job = CallAPI.CallAPI(app_config, server, token)

    def check_health(self):
        return_obj = self.api_call_job.call_api("health_check")
        if return_obj["status"] != "ok":
            raise Exception("FME Server is not healthy.")
        return True

    def list_repos(self):
        return_obj = self.api_call_job.call_api("list_repos")
        items = return_obj["items"]
        return items

    def list_repo_sub_items(self, repo_name):
        return_obj = self.api_call_job.call_api("list_repo_subs", [repo_name])
        items = return_obj["items"]
        return items

    def get_repo_datasets_info(self, repo_name, fmw_name, dataset_dir, dataset_name):
        dataset_info = self.api_call_job.call_api("get_repo_datasets_info",
                                                  [repo_name, fmw_name, dataset_dir, dataset_name])
        return dataset_info

    def list_repo_datasets_features(self, repo_name, fmw_name, dataset_dir, dataset_name):
        feature_list = self.api_call_job.call_api("list_repo_datasets_features",
                                                  [repo_name, fmw_name, dataset_dir, dataset_name])
        return feature_list

    def get_repo_datasets_feature_info(self, repo_name, fmw_name, dataset_dir, dataset_name, feature_name):
        feature_info = self.api_call_job.call_api("get_repo_datasets_feature_info", [repo_name, fmw_name, dataset_dir,
                                                                                     dataset_name, feature_name])
        return feature_info

    def get_repo_datasets(self, repo_name, fmw_name, deep=False):
        src_dataset_list = self.api_call_job.call_api("list_repo_datasets", [repo_name, fmw_name, "source"])
        dest_dataset_list = self.api_call_job.call_api("list_repo_datasets", [repo_name, fmw_name, "destination"])
        return {"source_datasets": src_dataset_list, "destination_datasets": dest_dataset_list}
