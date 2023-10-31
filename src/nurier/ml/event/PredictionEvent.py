from src.nurier.ml.event.EventExtends import EventExtends
from src.nurier.fds.common.Message import Message

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

from multipledispatch import dispatch
import time
import json


class PredictionEvent(EventExtends):

    message: Message
    message_method_map: dict
    row_message: DataFrame
    original_message: json = ""
    prediction: DataFrame
    prediction_result: bool = False
    prediction_value: float = 0.0
    json: json = ""

    def __init__(self, message: Message):
        self.message = message
        self.receive_nano_time = time.time_ns()

        if self.message.Amount == 0.0:
            self.message.Amount = 0

        self.message_method_map = self.getter_map(message)

    def set_original_message(self, value):
        self.original_message = value

    @dispatch()
    def set_prediction_result(self):
        if self.prediction is None:
            self.prediction_result = False
            self.prediction_value = 0.0
        else:
            if self.prediction.filter(col("prediction") == 1.0).count() >= 1:
                self.prediction_result = True
                self.prediction_value = 1.0
            else:
                self.prediction_result = False
                self.prediction_value = 0.0

    @dispatch(float)
    def set_prediction_result(self, value):
        self.prediction_value = value

        if self.prediction_value == 1.0:
            self.prediction_result = True
            self.prediction_value = 1.0
        else:
            self.prediction_result = False
            self.prediction_value = 0.0

    def get_prediction_code(self):
        if self.prediction_value == 1.0:
            return "1"
        else:
            return "0"
