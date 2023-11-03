from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.ml.data.DataStructType import ML_Field
from src.nurier.ml.data.DataObject import DataObject
from src.nurier.ml.data.SparkDataObject import SparkDataObject

from logging import Logger
from multipledispatch import dispatch
import time

from pyspark.ml import PipelineModel, Pipeline
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.classification import LogisticRegression, LinearSVC, RandomForestClassifier, MultilayerPerceptronClassifier,\
    GBTClassifier, DecisionTreeClassifier, OneVsRest, NaiveBayes
from pyspark.ml.regression import LinearRegression


class FDSPredictionModel:

    prediction_data: DataFrame
    pipeline_model: PipelineModel
    media_group_name: str
    media_group_code_array: list
    struct_type: StructType

    def __init__(self, log, type_name, model_name, media_group_name, media_group_code_array):
        self.log: Logger = log
        self.type_name = type_name
        self.model_name = model_name
        self.media_group_name = media_group_name
        self.media_group_code_array = media_group_code_array

    @dispatch(str, list)
    def data_loading(self, file_path):
        file_path = file_path if file_path is not None and file_path.__ne__("") else f"{prop().get_model_result_file_path()}{self.media_group_name}_{self.model_name}"
        self.load_model(f"{file_path}_model")
        use_field = ML_Field.get_apply_columnList(self.media_group_name)
        self.struct_type = self.get_struct_type_list(use_field)
        self.log.debug(f"Data Loading : {self.media_group_name}")

    @dispatch(str, int, float, bool, float, int, int, int, str)
    def data_loading(self, file_path, model_param_maxiter, model_param_reg, model_param_fitintercept, model_param_elasticnet
                     , model_param_maxdepth, model_param_layeroptions, model_param_blocksize, model_scaler):
        data = self.get_model_training_data(file_path, model_scaler)
        use_field = ML_Field.get_apply_columnList(self.media_group_name)
        self.struct_type = self.get_struct_type_list(use_field)
        self.log.debug(f"Data Loading : {self.media_group_name}")

        pipeline_stages = data.get_pipeline_array()
        model_type = self.get_model_type(model_param_maxiter, model_param_reg, model_param_fitintercept, model_param_elasticnet
                            , model_param_maxdepth, model_param_layeroptions, model_param_blocksize)
        pipeline_stages.append(model_type)
        pipeline = Pipeline().setStages(pipeline_stages)
        self.log.debug(f"Model Fitting ( Training ) Start")
        model_training_time = time.time()
        self.pipeline_model = pipeline.fit(data.get_train_data())
        self.log.debug(f"Model Fitting ( Training ) End - {(time.time() - model_training_time):.5f}sec")

    def load_model(self, file_path):
        try:
            self.pipeline_model = PipelineModel.read().load(file_path)
        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")
            self.log.error(f"{self.__class__} ERROR : {e}")

    def result_prediction(self, data: DataFrame):
        start = time.time()
        try:
            print("Result Prediction : PipeLine Model transform")
            self.prediction_data = self.pipeline_model.transform(data)
            return self.prediction_data
        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")
        finally:
            self.log.debug(f"Result Prediction Time : {(time.time() - start):.5f}sec")

    def get_struct_type_list(self, use_field):
        fieldList = []
        for field in use_field:
            fieldList.append(StructField(field.key, field.type, False))
        return StructType(fieldList)

    def print_struct_type(self):
        self.log.debug(self.struct_type.jsonValue())

    def get_model_type(self, model_param_maxiter, model_param_reg, model_param_fitintercept, model_param_elasticnet
                       , model_param_maxdepth, model_param_layeroptions, model_param_blocksize):
        label = ML_Field.get_label()
        if self.model_name.__eq__("LinearRegression"):
            linear_regression = LinearRegression()
            linear_regression.setMaxIter(model_param_maxiter)\
                .setRegParam(model_param_reg)\
                .setFitIntercept(model_param_fitintercept)\
                .setElasticNetParam(model_param_elasticnet)\
                .setFeaturesCol("features")\
                .setLabelCol(label)
            return linear_regression
        elif self.model_name.__eq__("LogisticRegression"):
            logistic_regression = LogisticRegression()
            logistic_regression.setMaxIter(model_param_maxiter)\
                .setRegParam(model_param_reg)\
                .setFitIntercept(model_param_fitintercept)\
                .setElasticNetParam(model_param_elasticnet)\
                .setFeaturesCol("features")\
                .setLabelCol(label)
            return logistic_regression
        elif self.model_name.__eq__("DecisionTreeClassifier"):
            decisiontree_classifier = DecisionTreeClassifier()
            decisiontree_classifier.setMaxBins(prop().get_model_max_beans())\
                .setMaxDepth(model_param_maxdepth)\
                .setFeaturesCol("features")\
                .setLabelCol(label)
            return decisiontree_classifier
        elif self.model_name.__eq__("LinearSVC"):
            linear_svc = LinearSVC()
            linear_svc.setMaxIter(model_param_maxiter)\
                .setFitIntercept(model_param_fitintercept)\
                .setRegParam(model_param_reg)\
                .setFeaturesCol("features")\
                .setLabelCol(label)
            return linear_svc
        elif self.model_name.__eq__("NaiveBayes"):
            naive_bayes = NaiveBayes()
            naive_bayes.setFeaturesCol("features")\
                .setLabelCol(label)
            return naive_bayes
        elif self.model_name.__eq__("RandomForestClassifier"):
            randomforest_classifier = RandomForestClassifier()
            randomforest_classifier.setMaxBins(prop().get_model_max_beans())\
                .setMaxDepth(model_param_maxdepth)\
                .setFeaturesCol("features")\
                .setLabelCol(label)
            return randomforest_classifier
        elif self.model_name.__eq__("GradientBoostedTreeClassifier"):
            gradientboostedtree_classifier = GBTClassifier()
            gradientboostedtree_classifier.setMaxBins(prop().get_model_max_beans())\
                .setMaxDepth(model_param_maxdepth)\
                .setFeaturesCol("features")\
                .setLabelCol(label)
            return gradientboostedtree_classifier
        elif self.model_name.__eq__("MultilayerPerceptronClassifier"):
            multilayer_perceptron = MultilayerPerceptronClassifier()
            multilayer_perceptron.setLayers(model_param_layeroptions)\
                .setBlockSize(model_param_blocksize)\
                .setMaxIter(model_param_maxiter)\
                .setSeed(10)\
                .setFeaturesCol("features")\
                .setLabelCol(label)
            return multilayer_perceptron
        elif self.model_name.__eq__("OneVsRestClassifier"):
            one_vs_rest = OneVsRest()
            classifier = LogisticRegression().setMaxIter(model_param_maxiter) \
                .setTol(1E-6) \
                .setFitIntercept(True)
            one_vs_rest.setClassifier(classifier)\
                .setFeaturesCol("features")\
                .setLabelCol(label)
            return one_vs_rest
        else:
            return None

    def get_model_training_data(self, file_name, scaler):
        do = DataObject(file_name)
        do.set_media_group_name(self.media_group_name)
        do.set_media_group_code_array(self.media_group_code_array)

        sdo = SparkDataObject(do)

        # Category Indexer
        sdo.append_category_indexer(ML_Field.get_category_indexerList(self.media_group_name))

        # Scaler
        sdo.append_features("tempScalerFeatures", ML_Field.get_scalerList(self.media_group_name))
        if scaler.__eq__("Normalizer"):
            sdo.append_normalizer("tempScalerFeatures", "ScalerFeatures", 2.0)
        elif scaler.__eq__("MaxAbsScaler"):
            sdo.append_maxabs_scaler("tempScalerFeatures", "ScalerFeatures")
        elif scaler.__eq__("MinMaxScaler"):
            sdo.append_minmax_scaler("tempScalerFeatures", "ScalerFeatures")
        elif scaler.__eq__("StandardScaler"):
            sdo.append_standard_scaler("tempScalerFeatures", "ScalerFeatures", True, False)
        elif scaler.__eq__("RobustScaler"):
            sdo.append_robust_scaler("tempScalerFeatures", "ScalerFeatures")
        else:
            sdo.append_standard_scaler("tempScalerFeatures", "ScalerFeatures", True, False)

        # Features / Label
        sdo.append_features(ML_Field.get_featureList(self.media_group_name))
        sdo.set_label_column(ML_Field.get_label())

        # Data Split
        sdo.set_split_data(0.7, 1)
        return sdo

    def is_use_media_code(self, media_code: str) -> bool:
        return self.media_group_code_array.__contains__(media_code)
