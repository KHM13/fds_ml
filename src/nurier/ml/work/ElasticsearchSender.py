from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.ml.common.SparkCommon import SparkCommon as scommon
from src.nurier.ml.event.PredictionEvent import PredictionEvent
from pyspark.sql import SQLContext

# from elasticsearch import Elasticsearch
# from elasticsearch.helpers import bulk
import time
import json


class ElasticsearchSender:

    __instance = None

    def __init__(self):
        ElasticsearchSender.__instance = self
        server = prop().get_search_engine_ml_server_nodes()
        self.server_list: list = server.split(",")
        print(f"server list : {self.server_list}")
        # self.client = Elasticsearch(hosts=server_list)
        spark = scommon.getInstance()
        sc = spark.get_spark_session().sparkContext
        self.client = SQLContext(sc)


    @staticmethod
    def getInstance():
        if ElasticsearchSender.__instance is None:
            ElasticsearchSender()
        return ElasticsearchSender.__instance

    def run(self, *args):
        try:
            if args[0] is not None and isinstance(args[0], PredictionEvent):
                event: PredictionEvent = args[0]
                probability = event.prediction.select('probability')
                prediction = event.prediction.select('prediction')
                id = event.original_message['STD_GBL_ID']
                mlresult = {'mlmodelid': id, 'prediction': prediction, 'probability': probability[1]}
                print(f"[ElasticsearchSender] mlresult : {mlresult}")
                event.original_message['mlresult'] = mlresult
                print(f"[ElasticsearchSender] elasticsearch document : {event.original_message}")

                df = self.client.read.json(event.original_message)
                path = f"{prop().get_search_engine_index_name()}_{event.original_message['date']}/_doc"
                df.write.format("org.elasticsearch.spark.sql").mode("append") \
                    .option("es.resource", str(path)) \
                    .option("es.nodes", "http://192.168.0.42:9210") \
                    .option("es.net.http.auth.user", prop().get_search_engine_ml_server_user_name()) \
                    .option("es.net.http.auth.pass", prop().get_search_engine_ml_server_user_password()) \
                    .save()

            elif args[0] is not None and isinstance(args[0], list):
                start_time = time.time()
                event_list = args[0]

                for event_message in event_list:
                    prediction_dict = event_message.prediction.asDict()
                    probability = prediction_dict['probability']
                    prediction = prediction_dict['prediction']
                    id = event_message.original_message['STD_GBL_ID']
                    mlresult = {'mlmodelid': id, 'prediction': prediction, 'probability': probability[1]}
                    print(f"[ElasticsearchSender] mlresult : {mlresult}")
                    event_message.original_message['mlresult'] = json.dumps(mlresult)
                    # elastic_message = {"index": f"{prop().get_search_engine_index_name()}_{event_message.original_message['date']}",
                    #                    "id": id, "type": "_doc", "source": event_message.original_message}
                    print(f"[ElasticsearchSender] elasticsearch message : {event_message.original_message}")
                    # es_conf = {"es.nodes": self.server_list,
                    #            "es.mapping.id": id,
                    #            "es.resource": f"{prop().get_search_engine_index_name()}_{event_message.original_message['date']}/_doc",
                    #            "es.net.http.auth.user": prop().get_search_engine_ml_server_user_name(),
                    #            "es.net.http.auth.pass": prop().get_search_engine_ml_server_user_password()}
                    # df = self.client.read.json(event_message.original_message)
                    # path = f"{prop().get_search_engine_index_name()}_{event_message.original_message['date']}/_doc"
                    # print(df.show())
                    # print(f"path : {path}")
                    # df.write.format("org.elasticsearch.spark.sql").mode("append")\
                    #     .option("es.resource", str(path))\
                    #     .option("es.nodes", "http://192.168.0.42:9210")\
                    #     .option("es.net.http.auth.user", prop().get_search_engine_ml_server_user_name()) \
                    #     .option("es.net.http.auth.pass", prop().get_search_engine_ml_server_user_password()) \
                    #     .save()

                    sc = scommon.getInstance().get_spark_session()
                    print(f"original message type : {type(event_message.original_message)}")
                    if type(event_message.original_message) not in ["json", "jsonObject"]:
                        json_data = json.dumps(event_message.original_message)
                    else:
                        json_data = event_message.original_message
                    rdd_data = sc.sparkContext.parallelize([json_data])
                    print(rdd_data)
                    rdd_data.saveAsNewAPIHadoopFile(
                        path='-',
                        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                        keyClass="org.apache.hadoop.io.NullWritable",
                        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                        conf={
                            "es.nodes": self.server_list,
                            "es.mapping.id": id,
                            "es.resource": f"{prop().get_search_engine_index_name()}_{event_message.original_message['date']}/_doc",
                            "es.input.json": "true",
                            "es.net.http.auth.user": prop().get_search_engine_ml_server_user_name(),
                            "es.net.http.auth.pass": prop().get_search_engine_ml_server_user_password()
                        }
                    )
                    # bulk(self.client, elastic_message)

                print(f"[ElasticsearchSender] size : {len(event_list)}")
                print(f"[ElasticsearchSender] time : {time.time() - start_time:.5f}sec")
            else:
                print(f"[ElasticsearchSender] args : {type(args)}")
                print(f"[ElasticsearchSender] value : {str(args)}")
        except Exception as e:
            print(f"[ElasticsearchSender] ERROR : {e}")
