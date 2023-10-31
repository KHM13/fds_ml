from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.ml.data.DataObject import DataObject
from src.nurier.ml.data.SparkDataObject import SparkDataObject
from src.nurier.ml.data.DataStructType import ML_Field
from src.nurier.ml.evaluation.EvaluationPrediction import EvaluationPrediction
from src.nurier.ml.model import FDSOneVsRestClassifier, FDSDecisionTreeClassifier, FDSGradientBoostingRegressor, FDSRandomForestClassifier
from src.nurier.ml.model import FDSLogisticRegression, FDSNaiveBayes, FDSLinearSVC, FDSLinearRegression, FDSMultilayerPerceptronClassifier
from multipledispatch import dispatch

import os
import warnings

warnings.filterwarnings("ignore")


@dispatch(str)
def get_model_training_data(media_group):
    # media_group[0]: mediaGroupName: IB / SB
    # media_group[1]: mediaGroupCodeArray: 매체코드 리스트(구분자: ',' )
    media_group_name = media_group.split(":")[0]
    media_group_code_array = media_group.split(":")[1].split(",")
    file_name = prop().get_prediction_ib_training_data()
    if media_group_name.__eq__("SB"):
        file_name = prop().get_prediction_sb_training_data()

    return get_model_training_data(file_name, media_group_name, media_group_code_array, prop().get_preprocess_execute_scaler())


@dispatch(str, str, list, str)
def get_model_training_data(file_name, media_group_name, media_group_code_array, scaler):
    do = DataObject(file_name)
    do.set_media_group_name(media_group_name)
    do.set_media_group_code_array(media_group_code_array)

    sdo = SparkDataObject(do)
    sdo.set_label_column(ML_Field.get_label())

    # Category Indexer
    sdo.append_category_indexer(ML_Field.get_category_indexerList(media_group_name))

    # Scaler
    sdo.append_features("tempScalerFeatures", ML_Field.get_scalerList(media_group_name))
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
    sdo.append_features(ML_Field.get_featureList(media_group_name))
    sdo.set_label_column(ML_Field.get_fields())

    # Data Split
    sdo.set_split_data(0.7, 1)
    return sdo


def execute_LinearRegression(data: SparkDataObject, label: str):
    train_size = prop().get_model_training_size()
    fds_model = FDSLinearRegression.FDSLinearRegression(data).create().set_model(train_size, 0.5, label)
    # fds_model = FDSLinearRegression.FDSLinearRegression(data).create().set_model(0.4, 100, 0.01, 0.1, True, label)
    fds_model.training()
    fds_model.predicting()
    fds_model.prediction_transform_bucketizer()
    return fds_model


def execute_LogisticRegression(data: SparkDataObject, label: str):
    train_size = prop().get_model_training_size()
    fds_model = FDSLogisticRegression.FDSLogisticRegression(data).create().set_model(train_size, label)
    # fds_model = FDSLogisticRegression.FDSLogisticRegression(data).create().set_model(100, 0.01, 0.1, True, label)
    fds_model.training()
    fds_model.predicting()
    return fds_model


def execute_DecisionTreeClassifier(data: SparkDataObject, label: str):
    train_size = prop().get_model_training_size()
    fds_model = FDSDecisionTreeClassifier.FDSDecisionTreeClassifier(data).create().set_model(train_size, label)
    fds_model.training()
    fds_model.predicting()
    return fds_model


def execute_LinearSVC(data: SparkDataObject, label: str):
    train_size = prop().get_model_training_size()
    fds_model = FDSLinearSVC.FDSLinearSVC(data).create().set_model(train_size, label)
    fds_model.training()
    fds_model.predicting()
    return fds_model


def execute_NaiveBayes(data: SparkDataObject, label: str):
    fds_model = FDSNaiveBayes.FDSNaiveBayes(data).create().set_model(label)
    fds_model.training()
    fds_model.predicting()
    return fds_model


def execute_RandomForestClassifier(data: SparkDataObject, label: str):
    train_size = prop().get_model_training_size()
    fds_model = FDSRandomForestClassifier.FDSRandomForestClassifier(data).create().set_model(train_size, label)
    fds_model.training()
    fds_model.predicting()
    return fds_model


def execute_GradientBoostedTreeClassifier(data: SparkDataObject, label: str):
    train_size = prop().get_model_training_size()
    fds_model = FDSGradientBoostingRegressor.FDSGradientBoostedTreeClassifier(data).create().set_model(train_size, label)
    fds_model.training()
    fds_model.predicting()
    return fds_model


def execute_MultilayerPerceptronClassifier(data: SparkDataObject, label: str):
    fds_model = FDSMultilayerPerceptronClassifier.FDSMultilayerPerceptronClassifier(data).create().set_model(128, 100, label)
    fds_model.training()
    fds_model.predicting()
    return fds_model


def execute_OneVsRestClassifier(data: SparkDataObject, label: str):
    fds_model = FDSOneVsRestClassifier.FDSOneVsRestClassifier(data).create().set_model(100, label)
    fds_model.training()
    fds_model.predicting()
    return fds_model


if __name__ == '__main__':
    os.system('chcp 65001')

    for media_group in prop().get_preprocess_media_type_group():
        data = get_model_training_data(media_group)
        media_group_name = media_group.split(":")[0]

        execute_model = prop().get_model_training_execute_model_list().split(",")
        label_column = ML_Field.get_label()

        for model_name in execute_model:
            print(f"pipeline array : {data.get_pipeline_array()}")

            if model_name.__eq__("LinearRegression"):
                fds_model = execute_LinearRegression(data, label_column)
            elif model_name.__eq__("LinearSVC"):
                fds_model = execute_LinearSVC(data, label_column)
            elif model_name.__eq__("LogisticRegression"):
                fds_model = execute_LogisticRegression(data, label_column)
            elif model_name.__eq__("DecisionTreeClassifier"):
                fds_model = execute_DecisionTreeClassifier(data, label_column)
            elif model_name.__eq__("RandomForestClassifier"):
                fds_model = execute_RandomForestClassifier(data, label_column)
            elif model_name.__eq__("GradientBoostedTreeClassifier"):
                fds_model = execute_GradientBoostedTreeClassifier(data, label_column)
            elif model_name.__eq__("NaiveBayes"):
                fds_model = execute_NaiveBayes(data, label_column)
            elif model_name.__eq__("MultilayerPerceptronClassifier"):
                fds_model = execute_MultilayerPerceptronClassifier(data, label_column)
            elif model_name.__eq__("OneVsRestClassifier"):
                fds_model = execute_OneVsRestClassifier(data, label_column)
            else:
                fds_model = execute_RandomForestClassifier(data, label_column)

            if fds_model is not None and prop().is_model_report_save():
                evaluation = EvaluationPrediction(fds_model)
                result = evaluation.print_evaluation_model()
                fds_model.log.debug(result)

            if fds_model is not None and prop().is_model_result_save():
                file_path = f"{prop().get_model_result_file_path()}{media_group_name}_{fds_model.model_title}"
                fds_model.save_pipeline(file_path)
                fds_model.save_model(f"{file_path}_model")

            data.pipeline_array.pop(-1)     # TrainValidSplit Remove
