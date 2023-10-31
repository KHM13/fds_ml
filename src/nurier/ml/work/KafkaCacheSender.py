from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.ml.event.PredictionEvent import PredictionEvent
from pyspark.sql.dataframe import DataFrame
from kafka import KafkaProducer
import json
import time


class KafkaCacheSender:

    __instance = None

    def __init__(self):
        self.topic_name = prop().get_sender_engine_kafka_ml_topic_name()
        self.group_id = prop().get_sender_engine_kafka_ml_group_id()
        self.server = prop().get_sender_engine_ml_servers()
        self.producer = KafkaProducer(
            bootstrap_servers=self.server,
            value_serializer=lambda v: json.dumps(v).encode('UTF-8')
        )
        KafkaCacheSender.__instance = self

    @staticmethod
    def getInstance():
        if KafkaCacheSender.__instance is None:
            KafkaCacheSender()
        return KafkaCacheSender.__instance

    def run(self, *args):
        try:
            if args[0] is not None and isinstance(args[0], PredictionEvent):
                event: PredictionEvent = args[0]
                probability = event.prediction.select('probability')
                prediction = event.prediction.select('prediction')
                id = event.original_message['STD_GBL_ID']
                mlresult = {'mlmodelid': id, 'prediction': prediction, 'probability': probability[1]}
                print(f"[KafkaCacheSender] mlresult : {mlresult}")
                event.original_message['mlresult'] = mlresult
                elastic_message = {"index": f"nacf_{event.original_message['date']}", "id": id, "source": event.original_message, "type": "_doc"}
                print(f"[KafkaCacheSender] elasticsearch message : {elastic_message}")
                self.producer.send(topic=self.topic_name, value=elastic_message)

            elif args[0] is not None and isinstance(args[0], list):
                start_time = time.time()
                event_list = args[0]
                idx = 0

                for event_message in event_list:
                    prediction_dict = event_message.prediction.asDict()
                    probability = prediction_dict['probability']
                    prediction = prediction_dict['prediction']
                    id = event_message.original_message['STD_GBL_ID']
                    mlresult = {'mlmodelid': id, 'prediction': prediction, 'probability': probability[1]}
                    print(f"[KafkaCacheSender] mlresult : {mlresult}")
                    event_message.original_message['mlresult'] = mlresult
                    elastic_message = {"_index": f"nacf_{event_message.original_message['date']}", "_id": id, "_source": event_message.original_message, "_type": "_doc"}
                    print(f"[KafkaCacheSender] elasticsearch message : {elastic_message}")
                    self.producer.send(topic=self.topic_name, value=elastic_message)
                    idx += 1

                print(f"[KafkaCacheSender] size : {len(event_list)}")
                print(f"[KafkaCacheSender] time : {time.time() - start_time:.5f}sec")
            else:
                print(f"[KafkaCacheSender] args : {type(args)}")
                print(f"[KafkaCacheSender] value : {str(args)}")
        except Exception as e:
            print(f"[KafkaCacheSender] ERROR : {e}")
        finally:
            self.producer.flush()
            print("[KafkaCacheSender] producer flush")

