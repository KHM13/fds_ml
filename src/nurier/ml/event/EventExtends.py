class EventExtends:

    def getter_method(self, cls, method):
        try:
            return cls.__getattribute__(method)
        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")

    def getter_map(self, cls):
        result_map: dict = {}
        for field in dir(cls):
            if not field.startswith("_"):
                result_map.__setitem__(field, cls.__getattribute__(field))
        return result_map
