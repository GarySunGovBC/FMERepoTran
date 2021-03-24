from FMEAPI.ApiException import APIException


class ObjectCompare:

    @staticmethod
    def get_name(objects):
        return objects['name']

    @staticmethod
    def get_caption(objects):
        return objects['caption']

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
