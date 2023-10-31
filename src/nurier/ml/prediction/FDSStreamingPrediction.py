from src.nurier.ml import predictDataPreprocessing
from src.nurier.ml.common.mySqlCommon import mysqlCommon
from src.nurier.ml.prediction.FDSPrediction import FDSPrediction
from pyspark.sql.dataframe import DataFrame



class FDSStreamingPrediction:
    __pred: FDSPrediction
    __preList: list
    __instance = None

    def __init__(self):
        self.__pred = FDSPrediction()
        FDSStreamingPrediction.__instance = self
        """
        mysql = mysqlCommon()
        mysql.excute_query("table_name")
        self.__preList = mysql.get_result()
        """
    @staticmethod
    def getInstance():
        if FDSStreamingPrediction.__instance is None:
            FDSStreamingPrediction()
        return FDSStreamingPrediction.__instance

    ##############################################################
    # predicting method
    # 아래서 배치로 받은 DF를 모델에 예측 한 뒤 결과 레디스, 엘라에 전송
    ##############################################################
    @staticmethod
    def predicting(df: DataFrame, epoch_id):

        ###############################################################
        # preprocessing
        # df(Spark df) => pd_df => preprocess(sklearn, ...) => Spark df
        ###############################################################

        cp = False

        test = [["1", "TR_DTM", "except", "", "", ""],
                ["2", "indexName", "missing", "input", "0", ""],
                ["2", "documentTypeName", "missing", "input", "0", ""],
                ["2", "COPR_DS", "missing", "input", "0", ""],
                ["2", "LANG_DS", "missing", "input", "0", ""],
                ["2", "E_FNC_USR_OS_DSC", "missing", "input", "0", ""],
                ["2", "E_FNC_LGIN_DSC", "missing", "input", "0", ""],
                ["2", "NBNK_C", "missing", "input", "0", ""]]

        if cp:
            preList = FDSStreamingPrediction.__preList
            pd_df = df.toPandas()
            df = predictDataPreprocessing.preprocess(pd_df, preList)

        df.cache()
        # 인뱅 스뱅 분할
        ib_kafka_df = df.filter((df.EBNK_MED_DSC == "091") | (df.EBNK_MED_DSC == "070"))
        sb_kafka_df = df.filter((df.EBNK_MED_DSC != "091") & (df.EBNK_MED_DSC != "070"))

        ib_ch, sb_ch = True, True

        if ib_kafka_df.isEmpty():
            ib_ch = False
        if sb_kafka_df.isEmpty():
            sb_ch = False

        ib_model = FDSStreamingPrediction.getInstance().__pred.model_IB
        sb_model = FDSStreamingPrediction.getInstance().__pred.model_SB

        ##############################
        # fitting result => redis, ela
        ##############################
        if ib_ch:
            # row_list = [ib_kafka_df]
            ib_model = ib_model.result_prediction(ib_kafka_df)
            print("ib_kafka_df: ", ib_model.show())
            # UpdatePrediction.getInstance().process(row_list, ib_model)
        if sb_ch:
            # row_list = [sb_kafka_df]
            sb_model = sb_model.result_prediction(sb_kafka_df)
            print("sb_kafka_df: ", sb_model.show())
            # UpdatePrediction.getInstance().process(row_list, sb_model)
