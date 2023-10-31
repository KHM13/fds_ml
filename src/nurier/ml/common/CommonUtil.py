from datetime import datetime
from src.nurier.fds.common.LoggingHandler import LoggingHandler
from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.ml.event import PredictionEvent


class CommonUtil:
    __FASTDATE_TYPE_0 = "%Y-%m-%d_%H:%M:%S.%f"
    __FASTDATE_TYPE_1 = "%Y-%m-%d_%H:%M:%S"
    __FASTDATE_TYPE_2 = "%Y%m%d%H%M%S"
    __FASTDATE_TYPE_3 = "%Y%m%d"

    def __init__(self):
        self.logger = LoggingHandler(f"{prop().get_result_log_file_path()}{self.now_type3()}_output", "a", "DEBUG")
        self.logger = self.logger.get_log()

    def log_time(self):
        return self.__now(self.__FASTDATE_TYPE_0)

    def now(self):
        return self.__now(self.__FASTDATE_TYPE_1)

    def now_type2(self):
        return self.__now(self.__FASTDATE_TYPE_2)

    def now_type3(self):
        return self.__now(self.__FASTDATE_TYPE_3)

    def __now(self, format_type: str):
        return datetime.now().strftime(format_type)

    @staticmethod
    def get_int_from_string(value: str) -> int:
        if value.isnumeric():
            return int(value)
        else:
            return 0

    @staticmethod
    def get_float_from_string(value: str) -> float:
        value = value.replace(".", "")
        if value.isnumeric():
            return float(value)

    @staticmethod
    def convert_string(array_string) -> str:
        return ', '.join(array_string)

    @staticmethod
    def check_null_string(input: str) -> str:
        if not(input and input.strip()):
            return ''
        elif input.strip() == 'null':
            return ''
        else:
            input.strip()

    @staticmethod
    def get_model_training_drop_column_list(mediacode: str) -> str:
        if mediacode == "IB":
            return prop().get_model_training_drop_column_ib_list()
        elif mediacode == "SB":
            return prop().get_model_training_drop_column_sb_list()
        else:
            return ""

    def print_catch_exception(self, e: exec, message: PredictionEvent):
        self.logger.error("##########################################")
        self.logger.error("### Exception Start")
        self.logger.error(e)
        if message is None:
            self.logger.error("json : {}".format(message.getJson()))
            self.logger.error("message : {}".format(message.getMessage()))
        print(f"{self.__class__} ERROR : {e}")
        self.logger.error("### Exception End")
        self.logger.error("##########################################")
