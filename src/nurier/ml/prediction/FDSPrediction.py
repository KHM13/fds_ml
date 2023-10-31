from src.nurier.ml.prediction.FDSPredictionModel import FDSPredictionModel
from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.fds.common.LoggingHandler import LoggingHandler

logger = LoggingHandler(f"{prop().get_result_log_file_path()}FDSPrediction", "a", "DEBUG")


class FDSPrediction:

    model_IB: FDSPredictionModel
    model_SB: FDSPredictionModel

    def __init__(self):
        log = logger.get_log()
        self.model_IB = FDSPredictionModel(log, prop.type_name_IB, prop().get_prediction_ib_model_name(), "IB", prop().get_prediction_ib_model_media_type_group())
        if prop().get_prediction_ib_is_load():
            self.model_IB.data_loading("D:/fds_ml\output\model/IB_RandomForestClassifier_model")
        else:
            self.model_IB.data_loading(
                prop().get_prediction_ib_training_data(),
                prop().get_prediction_ib_model_param_maxiter(),
                prop().get_prediction_ib_model_param_reg(),
                prop().get_prediction_ib_model_param_fit_intercept(),
                prop().get_prediction_ib_model_param_elasticnet(),
                prop().get_prediction_ib_model_param_maxdepth(),
                prop().get_prediction_ib_model_param_layer_options(),
                prop().get_prediction_ib_model_param_block_size(),
                prop().get_prediction_ib_model_execute_scaler()
            )
        self.model_IB.log.debug("## model_IB.getStructType().printTreeString() ##")
        self.model_IB.print_struct_type()

        log.debug("")

        self.model_SB = FDSPredictionModel(log, prop.type_name_SB, prop().get_prediction_sb_model_name(), "SB", prop().get_prediction_sb_model_media_type_group())
        if prop().get_prediction_sb_is_load():
            self.model_SB.data_loading("D:/fds_ml\output\model/SB_RandomForestClassifier_model")
        else:
            self.model_SB.data_loading(
                prop().get_prediction_sb_training_data(),
                prop().get_prediction_sb_model_param_maxiter(),
                prop().get_prediction_sb_model_param_reg(),
                prop().get_prediction_sb_model_param_fit_intercept(),
                prop().get_prediction_sb_model_param_elasticnet(),
                prop().get_prediction_sb_model_param_maxdepth(),
                prop().get_prediction_sb_model_param_layer_options(),
                prop().get_prediction_sb_model_param_block_size(),
                prop().get_prediction_sb_model_execute_scaler()
            )
        self.model_SB.log.debug("## model_SB.getStructType().printTreeString() ##")
        self.model_SB.print_struct_type()

        log.debug("")

    def get_model(self, media_code):
        if self.model_IB.is_use_media_code(media_code):
            return self.model_IB
        elif self.model_SB.is_use_media_code(media_code):
            return self.model_SB
        else:
            return self.model_IB    # default

    def get_struct_type(self, media_code):
        if self.model_IB.is_use_media_code(media_code):
            return self.model_IB.struct_type
        elif self.model_SB.is_use_media_code(media_code):
            return self.model_SB.struct_type
        else:
            return self.model_IB.struct_type    # default
