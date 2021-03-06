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
