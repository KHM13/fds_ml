from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.fds.common.LoggingHandler import LoggingHandler
from src.nurier.ml.data.FeatureSelectProcess import FeatureSelectProcess
from src.nurier.ml.data.DataObject import DataObject
from src.nurier.ml.data.DataStructType import ML_Field

if __name__ == '__main__':
    logger = LoggingHandler(f"{prop().get_result_log_file_path()}{prop().get_feature_select_log_file_name()}", "w", "DEBUG")
    log = logger.get_log()

    label = ML_Field.get_label()

    for media_group in prop().get_preprocess_media_type_group():
        media_group_name = media_group.split(":")[0]
        media_group_code_array = media_group.split(":")[1].split(",")

        log.debug("")
        log.debug(f"mediaGroupName : {media_group_name}")
        log.debug(f"mediaGroupCodeArray : {', '.join(media_group_code_array)}")

        do = DataObject(prop().get_preprocess_file_path(), f"{media_group_name}_{prop().get_preprocess_result_file_name()}")
        log.debug(f"Data shape : {do.get_data().shape}")

        do.set_media_group_name(media_group_name)
        do.set_media_group_code_array(media_group_code_array)

        # Category Indexer
        do.data_categorier([], True)
        dataset = do.get_data()

        # Feature Select
        fsp = FeatureSelectProcess(dataset)
        # process_type_name = "forward"
        # process_type_name = "backward"
        process_type_name = "stepwise"
        log.debug("")
        log.debug(f"변수 선택법 : {process_type_name}")
        result = {}
        if process_type_name.__eq__("forward"):
            result = fsp.forward_feature_select()
        elif process_type_name.__eq__("backward"):
            result = fsp.backward_feature_select()
        else:
            result = fsp.stepwise_feature_select()
        log.debug(f"Before Select Columns Size : {len(dataset.columns)}")
        log.debug(f"Best AIC : {result.get('aic')}")
        log.debug(f"Selected Columns : {len(result.get('columns'))}\n{','.join(result.get('columns'))}")
        log.debug(f"Best Model Summary : \n{result.get('summary')}")
