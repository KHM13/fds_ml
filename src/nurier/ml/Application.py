from src.nurier.ml.prediction.FDSStreamingPrediction import FDSStreamingPrediction
from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.ml.common.SparkCommon import SparkCommon as scommon
from src.nurier.ml.data.DataStructType import ML_Field
from pyspark.sql.functions import from_json
from pyspark.sql import SparkSession
import findspark


import os

if __name__ == '__main__':
    os.system('chcp 65001')
    findspark.init()

    prop().set_spark_app_name("FDS_ML_TEST")

    ###################################
    # model, preprocess, fitting
    ###################################
    fsp = FDSStreamingPrediction()

    ###################################
    # Spark_Session
    ###################################
    ss: SparkSession = scommon.getInstance().get_spark_session()

    ###################################
    # kafka_config
    ###################################
    kafka_username, kafka_password = 'admin', 'admin'
    kafka_topic_name = 'analysis-blockuser-cache'
    options = {
        "kafka.bootstrap.servers": "192.168.0.17:9092",

        "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{}" password="{}";'.format(
            kafka_username, kafka_password),
        "subscribe": kafka_topic_name,

        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "PLAINTEXT",
        "group.id": "hi",
        "startingOffsets": "earliest",
        "maxOffsetsPerTrigger": 100
    }

    #####################################
    # kafka => Spark structured Streaming
    #####################################
    lines = ss.readStream \
        .format("kafka") \
        .options(**options) \
        .load()

    kafka_df = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").select('value')
    kafka_df = kafka_df.withColumn("value", from_json(kafka_df["value"], ML_Field.get_schema())).select(
        "value.*").fillna(0)
    query = kafka_df.writeStream.outputMode("append").foreachBatch(fsp.predicting).start().awaitTermination()
