import json
from FMEAPI.ApiException import APIException
from FMERepositoryUtility.ObjectCompare import ObjectCompare


class FMWCompare:
    CONFIG = "fmw.json"

    def __init__(self, fmw1, fmw2):
        self.fmw1 = fmw1
        self.fmw2 = fmw2
        with open(FMWCompare.CONFIG) as fmw_config_json:
            self.fmw_config = json.load(fmw_config_json)

    @staticmethod
    def object_equal(objects, props):
        if len(props) == 0:
            return
        if isinstance(objects[0], dict):
            obj1 = objects[0]
            obj2 = objects[1]
            for prop in props:
                if prop not in obj1.keys() and prop not in obj2.keys():
                    continue
                if obj1[prop] != obj2[prop]:
                    raise APIException(prop)

    def props_equal(self, objects, prop_name):
        """ compare 2 objects, stored in objects, objects[0] and objects[1] """
        # compare objects, name defined in "props"
        ObjectCompare.object_equal(objects, self.fmw_config[prop_name]["props"])
        # if the single object is list
        if isinstance(objects[0], list):
            if len(objects[0]) != len(objects[1]):
                raise APIException(prop_name)
            # sort ths list before comparing
            # sort object list base on a fieldy
            sort_on = "name"
            if "sort_on" in self.fmw_config[prop_name].keys():
                sort_on = self.fmw_config[prop_name]["sort_on"]
            sorted_objects1 = None
            sorted_objects2 = None
            if sort_on == "name":
                sorted_objects1 = sorted(objects[0], key=ObjectCompare.get_name)
                sorted_objects2 = sorted(objects[1], key=ObjectCompare.get_name)
            if sort_on == "caption":
                sorted_objects1 = sorted(objects[0], key=ObjectCompare.get_caption)
                sorted_objects2 = sorted(objects[1], key=ObjectCompare.get_caption)
            # compare each
            for i in range(0, len(sorted_objects1)):
                obj1 = sorted_objects1[i]
                obj2 = sorted_objects2[i]
                prop_key = "%s_item" % prop_name
                self.props_equal([obj1, obj2], prop_key)
        # if the single object is dict
        if isinstance(objects[0], dict):
            if len(objects[0]) != len(objects[1]):
                raise APIException(prop_name)
            # "source" and "destination", as dir, have same props
            is_dir = "dir" in self.fmw_config[prop_name].keys()
            # compare each
            for sub_prop in self.fmw_config[prop_name]["sub_props"]:
                # some objects don't have the defined prop
                if sub_prop not in objects[0].keys() and sub_prop not in objects[1].keys():
                    continue
                sub_obj1 = objects[0][sub_prop]
                sub_obj2 = objects[1][sub_prop]
                # some dict objects are {}
                if len(sub_obj1) == 0 and len(sub_obj2) == 0:
                    continue
                prop_key = "%s_dir" % prop_name
                if not is_dir:
                    prop_key = "%s_%s" % (prop_name, sub_prop)
                self.props_equal([sub_obj1, sub_obj2], prop_key)

    # compare resoure
    def compare(self):
        """ compare 2 fmw, entry point is 'fmw' """
        self.props_equal([self.fmw1, self.fmw2], "fmw")
