from src.nurier.ml.prediction.FDSPrediction import FDSPrediction
from src.nurier.ml.work.UpdatePrediction import UpdatePrediction
from src.nurier.ml.event.PredictionEvent import PredictionEvent
from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.ml.common.CommonUtil import CommonUtil as util
from functools import reduce
from pyspark.sql.dataframe import DataFrame
import threading
import time


class FDSPredictionSchedule:

    type_name: str
    prediction_model: FDSPrediction

    is_loop: bool = True

    row_list_IB: list
    row_list_SB: list

    def __init__(self, type_name):
        super().__init__()
        self.type_name = type_name
        self.prediction_model = FDSPrediction()
        self.row_list_IB = []
        self.row_list_SB = []

    # async def scheduler_start(self):
        # await self.run()
        # await asyncio.sleep(3.0)

    # async def run(self):
    def run(self):
        if self.type_name.__eq__(prop().type_name_IB):
            if len(self.row_list_IB) > 0:
                print(f"Row List IB size : {len(self.row_list_IB)}")
                print(str(self.row_list_IB))
                self.execute_prediction(self.use_data_IB(False, None), self.prediction_model.model_IB)
        if self.type_name.__eq__(prop().type_name_SB):
            if len(self.row_list_SB) > 0:
                print(f"Row List SB size : {len(self.row_list_SB)}")
                print(str(self.row_list_SB))
                self.execute_prediction(self.use_data_SB(False, None), self.prediction_model.model_SB)
        threading.Timer(interval=3.0, function=self.run).start()

    def execute_prediction(self, prediction_data_list, model):
        try:
            start_date_time = util().log_time()
            start_time_1 = time.time()
            start_time_2 = time.time()
            row_list = []
            print(f"start date time : {start_date_time}")

            for event in prediction_data_list:
                temp_frame: DataFrame = event.row_message
                row_list.append(temp_frame)
            end_time_1 = time.time() - start_time_2

            data = reduce(DataFrame.unionAll, row_list)
            row_list.clear()

            start_time_2 = time.time()
            end_time_2 = time.time() - start_time_2

            start_time_2 = time.time()
            result = model.result_prediction(data)
            end_time_3 = time.time() - start_time_2

            UpdatePrediction.getInstance().process(prediction_data_list, result)

            if self.type_name.__eq__(prop().type_name_IB):
                print(f"[{self.type_name}] size : {len(prediction_data_list)} / {len(self.row_list_IB)}")
            if self.type_name.__eq__(prop().type_name_SB):
                print(f"[{self.type_name}] size : {len(prediction_data_list)} / {len(self.row_list_SB)}")

            end_time_4 = time.time() - start_time_1
            print(f"[{self.type_name}] create List<Row> - {end_time_1:.5f}sec")
            print(f"[{self.type_name}] create Dataset<Row> - {end_time_2:.5f}sec")
            print(f"[{self.type_name}] prediction - {end_time_3:.5f}sec")
            print(f"[{self.type_name}] [END] total - {start_date_time} -> {end_time_4:.5f}sec")
        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")

    def use_data(self, event: PredictionEvent):
        if self.prediction_model.model_IB.is_use_media_code(event.message.EBNK_MED_DSC):
            self.use_data_IB(True, event)
        elif self.prediction_model.model_SB.is_use_media_code(event.message.EBNK_MED_DSC):
            self.use_data_SB(True, event)
        else:
            self.use_data_IB(True, event)

    def use_data_IB(self, flag, event):
        if flag:
            self.row_list_IB.append(event)
            return None
        else:
            if prop().get_prediction_schedule_datarow_max() < len(self.row_list_IB):
                result = self.row_list_IB[:prop().get_prediction_schedule_datarow_max()]
                self.row_list_IB = self.row_list_IB[prop().get_prediction_schedule_datarow_max():len(self.row_list_IB)]
            else:
                result = self.row_list_IB.copy()
                self.row_list_IB.clear()
            return result

    def use_data_SB(self, flag, event):
        if flag:
            self.row_list_SB.append(event)
            return None
        else:
            if prop().get_prediction_schedule_datarow_max() < len(self.row_list_SB):
                result = self.row_list_SB[:prop().get_prediction_schedule_datarow_max()]
                self.row_list_SB = self.row_list_SB[prop().get_prediction_schedule_datarow_max():len(self.row_list_SB)]
            else:
                result = self.row_list_SB.copy()
                self.row_list_SB.clear()
            return result
