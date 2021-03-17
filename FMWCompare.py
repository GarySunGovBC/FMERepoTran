import json
import os

import FMEAPI
from ApiException import APIException


class FMWCompare(object):
    params = {
        "fmw": {
            "props": [
                "name",
                "description",
                "repositoryName",
                "title",
                "type",
                "fileSize",
                "category",
                "buildNumber",
            ],
            "sub_props": ['datasets', 'parameters', 'properties'],
        },
        "fmw_datasets": {
            "props": [],
            "sub_props": ['destination', 'source'],
            "dir": True,
        },
        "fmw_datasets_dir": {
            "props": [],
            "sub_props": [],
        },
        "fmw_datasets_dir_item": {
            "props": [
                "name",
                "format",
            ],
            "sub_props": ['featuretypes', 'properties'],
        },
        'fmw_datasets_dir_item_featuretypes': {
            "props": [],
            "sub_props": [],
        },
        'fmw_datasets_dir_item_featuretypes_item': {
            "props": ['name', 'description'],
            "sub_props": ['attributes', 'properties'],
        },
        'fmw_datasets_dir_item_featuretypes_item_attributes': {
            "props": [],
            "sub_props": [],
        },
        'fmw_datasets_dir_item_featuretypes_item_attributes_item': {
            "props": ['name', 'width', 'type'],
            "sub_props": [],
        },
        'fmw_datasets_dir_item_featuretypes_item_properties': {
            "props": [],
            "sub_props": [],
        },
        'fmw_datasets_dir_item_properties': {
            "props": [],
            "sub_props": [],
        },
        'fmw_datasets_dir_item_properties_item': {
            "props": ['name', 'category', 'value'],
            "sub_props": ['attributes'],
        },
        'fmw_datasets_dir_item_properties_item_attributes': {
            "props": [],
            "sub_props": [],
        },
        'fmw_parameters': {
            "props": [],
            "sub_props": [],
        },
        'fmw_parameters_item': {
            "props": ['defaultValue', 'description', 'model', 'type'],
            "sub_props": ['listOptions', ],
        },
        'fmw_parameters_item_listOptions': {
            "props": [],
            "sub_props": [],
            "sort_on": "caption"
        },
        'fmw_parameters_item_listOptions_item': {
            "props": ['caption', 'value', ],
            "sub_props": [],
        },
        'fmw_properties': {
            "props": [],
            "sub_props": [],
        },
        'fmw_properties_item': {
            "props": ['name', 'category', 'value', ],
            "sub_props": ['attributes'],
        },
    }

    def __init__(self, fmw1, fmw2):
        self.fmw1 = fmw1
        self.fmw2 = fmw2

    @staticmethod
    def object_equal(objects, props):
        if len(props) == 0:
            return
        if isinstance(objects[0], dict):
            obj1 = objects[0]
            obj2 = objects[1]
            for prop in props:
                if obj1[prop] != obj2[prop]:
                    raise APIException(prop)


    @staticmethod
    def get_name(objects):
        return objects['name']

    @staticmethod
    def get_caption(objects):
        return objects['caption']

    @staticmethod
    def props_equal(objects, prop_name):
        FMWCompare.object_equal(objects, FMWCompare.params[prop_name]["props"])
        if isinstance(objects[0], list):
            if len(objects[0]) != len(objects[1]):
                raise APIException(prop_name)
            sort_on = "name"
            if "sort_on" in FMWCompare.params[prop_name].keys():
                sort_on = FMWCompare.params[prop_name]["sort_on"]
            sorted_objects1 = None
            sorted_objects2 = None
            if sort_on == "name":
                sorted_objects1 = sorted(objects[0], key=FMWCompare.get_name)
                sorted_objects2 = sorted(objects[1], key=FMWCompare.get_name)
            if sort_on == "caption":
                sorted_objects1 = sorted(objects[0], key=FMWCompare.get_caption)
                sorted_objects2 = sorted(objects[1], key=FMWCompare.get_caption)
            for i in range(0, len(sorted_objects1)):
                obj1 = sorted_objects1[i]
                obj2 = sorted_objects2[i]
                prop_key = "%s_item" % prop_name
                FMWCompare.props_equal([obj1, obj2], prop_key)
        if isinstance(objects[0], dict):
            is_dir = "dir" in FMWCompare.params[prop_name].keys()
            for sub_prop in FMWCompare.params[prop_name]["sub_props"]:
                if sub_prop not in objects[0].keys():
                    continue
                sub_obj1 = objects[0][sub_prop]
                sub_obj2 = objects[1][sub_prop]
                if len(sub_obj1) == 0 and len(sub_obj2) == 0:
                    continue
                prop_key = "%s_dir" % prop_name
                if not is_dir:
                    prop_key = "%s_%s" % (prop_name, sub_prop)
                FMWCompare.props_equal([sub_obj1, sub_obj2], prop_key)

    def compare(self):
        FMWCompare.props_equal([self.fmw1, self.fmw2], "fmw")
