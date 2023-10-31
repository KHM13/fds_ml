from pyspark.sql.dataframe import DataFrame
from src.nurier.fds.common.LoggingHandler import LoggingHandler
from src.nurier.ml.work.MLStorageSender import MLStorageSender
# from src.nurier.ml.work.KafkaCacheSender import KafkaCacheSender
from src.nurier.ml.work.ElasticsearchSender import ElasticsearchSender
from src.nurier.ml.work.ExecutorFixThread import ExecutorFixThread
from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.ml.common.CommonUtil import CommonUtil as comutil
import time

logger = LoggingHandler(f"{prop().get_model_report_file_path()}{comutil().now_type3()}_predictionResult", "a", "DEBUG")
log = logger.get_log()


class UpdatePrediction(ExecutorFixThread):

    __instance = None

    def __init__(self):
        super(UpdatePrediction, self).__init__(prop().get_update_prediction_fix_thread_count(), "UpdatePredictionThread")
        UpdatePrediction.__instance = self

    @staticmethod
    def getInstance():
        if UpdatePrediction.__instance is None:
            UpdatePrediction()
        return UpdatePrediction.__instance

    def work(self, *args):
        try:
            if len(args) == 1:
                print(args[0].__class__)
            elif len(args) == 2:
                if args[0] is not None and isinstance(args[0], list) and args[1] is not None and isinstance(args[1], DataFrame):
                    start_time = time.time()
                    event_list: list = args[0]
                    prediction_data: DataFrame = args[1]

                    i = 0
                    for row in prediction_data.collect():
                        try:
                            event_list.__getitem__(i).set_prediction_result(float(row.__getitem__("prediction")))
                            event_list.__getitem__(i).prediction = row
                            print(f"[UpdatePrediction] prediction : {row}")
                        except Exception as e:
                            print(f"[UpdatePrediction] Exception : {e}")
                            print(f"[UpdatePrediction] Exception prediction : {row.__getitem__('prediction')}")
                            event_list.__getitem__(i).set_prediction_result(0.0)
                        i += 1

                    # log_message = ["## Prediction Result DataFrame", ""]
                    # for prediction_row in prediction_data._jdf.showString(1000, 70, False).split("\n"):
                    #     log_message.append(prediction_row)
                    # log_message.append("")
                    #
                    # for lm in log_message:
                    #     log.debug(lm)

                    # prediction_data.show()

                    redis_sender = MLStorageSender.getInstance()
                    redis_sender.run(event_list)

                    # kafka_producer = KafkaCacheSender.getInstance()
                    # kafka_producer.run(event_list)

                    elastic_sender = ElasticsearchSender.getInstance()
                    elastic_sender.run(event_list)

                    print(f"[UpdatePrediction] collect scan - {time.time() - start_time:.5f}sec {self.waiting}")
                else:
                    print(f"{args[0].__class__} / {args[1].__class__}")

        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")
