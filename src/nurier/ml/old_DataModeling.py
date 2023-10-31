from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.fds.common.LoggingHandler import LoggingHandler
from src.nurier.ml.data.DataObject import DataObject
from src.nurier.ml.data.DataStructType import ML_Field
from src.nurier.ml.model.old import FDSSupportVectorClassifier, FDSDecisionTreeClassifier, FDSGradientBoostedTreeClassifier, FDSRandomForestClassifier
from src.nurier.ml.model.old import FDSLogisticRegression, FDSNaiveBayes, FDSKNeighborsClassifier
import warnings

warnings.filterwarnings("ignore")

if __name__ == '__main__':
    logger = LoggingHandler(f"{prop().get_result_log_file_path()}dataModeling_old", "w", "DEBUG")
    log = logger.get_log()

    label = ML_Field.get_label()

    for media_group in prop().get_preprocess_media_type_group():
        media_group_name = media_group.split(":")[0]
        media_group_code_array = media_group.split(":")[1].split(",")

        log.debug("")
        log.debug(f"mediaGroupName : {media_group_name}")
        log.debug(f"mediaGroupCodeArray : {', '.join(media_group_code_array)}")

        do = DataObject(prop().get_preprocess_file_path(), f"{media_group_name}_{prop().get_preprocess_result_file_name()}")
        log.debug(f"{prop().get_preprocess_file_path()}{media_group_name}_{prop().get_preprocess_result_file_name()}")
        log.debug(f"Data shape : {do.get_data().shape}")

        do.set_media_group_name(media_group_name)
        do.set_media_group_code_array(media_group_code_array)

        # 데이터 정규화
        # do.apply_standard_scaler([], True, media_group_name)
        # do.apply_robust_scaler([], True)
        # do.apply_minmax_scaler([], True, media_group_name)

        # 문자열 데이터 카테고리화
        # do.data_categorier([], True)

        # 학습 데이터 분할
        x_train, x_test, y_train, y_test = do.train_test_data_division(0.8)
        log.debug("학습 데이터 분할")
        log.debug(f"train : x - {x_train.shape}, y - {y_train.shape} | test : x - {x_test.shape}, y - {y_test.shape}")

        # 데이터 모델링
        classifiers = {
            "LogisticRegression": FDSLogisticRegression.FDSLogisticRegression(do),
            "DecisionTreeClassifier": FDSDecisionTreeClassifier.FDSDecisionTreeClassifier(do),
            "RandomForestClassifier": FDSRandomForestClassifier.FDSRandomForestClassifier(do),
            "GradientBoostingRegressor": FDSGradientBoostedTreeClassifier.FDSGradientBoostingRegressor(do),
            "KNeighborsClassifier": FDSKNeighborsClassifier.FDSKNeighborsClassifier(do),
            "SupportVectorClassifier": FDSSupportVectorClassifier.FDSSupportVectorClassifier(do),
            "NaiveBayes": FDSNaiveBayes.FDSNaiveBayes(do)
        }

        for model_name, classifier in classifiers.items():
            print(f"model name : {model_name}")
            parameter_dict = {}
            classifier.log.debug("")
            classifier.log.debug(f"mediaGroupName : {media_group_name}")
            classifier.log.debug(f"학습데이터 분할")
            classifier.log.debug(f"train : x - {x_train.shape}, y - {y_train.shape} | test : x - {x_test.shape}, y - {y_test.shape}")

            training = classifier.training_gridsearch_CV(parameter_dict)
            classifier.predicting_model(True)
            if model_name.__ne__("KNeighborsClassifier") and model_name.__ne__("SupportVectorClassifier") and model_name.__ne__("NaiveBayes"):
                classifier.feature_importances(20) if model_name.__ne__("LogisticRegression") else classifier.feature_importances2(20)
