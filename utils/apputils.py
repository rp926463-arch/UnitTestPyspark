import json


class AppUtils:
    @staticmethod
    def convertToDictionary(object):
        if isinstance(object, str):
            return json.loads(object)
        else:
            if object is not None:
                dictionary = object.__dict__
            else:
                dictionary = None
        return dictionary
