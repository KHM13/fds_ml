from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.fds.common.LoggingHandler import LoggingHandler
from src.nurier.ml.data.OutlierProcess import OutlierProcess
from src.nurier.ml.data.DataObject import DataObject
from src.nurier.ml.data.DataStructType import ML_Field

if __name__ == '__main__':
    logger = LoggingHandler(f"{prop().get_result_log_file_path()}outlierProcessing", "w", "DEBUG")
    log = logger.get_log()

    label = ML_Field.get_label()
    outlier_save = False

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

        # Outlier
        op = OutlierProcess(do.get_data())

        for column in ML_Field.get_list_for_inttype_columns(media_group_name):
            if column in op.get_data().columns and column.__ne__(label):
                outlier_result = op.detect_outlier(column)
                log.debug("")
                log.debug(f"이상치 - q1 : {outlier_result.get('q1')}, q3 : {outlier_result.get('q3')}, median : {outlier_result.get('median')}")
                outlier_size = len(outlier_result.get('index'))
                log.debug(f"이상치 갯수 : {column} -> {outlier_size}")
                if outlier_size == 0:
                    continue
                log.debug(f"이상치 제거 전 : {op.get_data().shape}")
                op.outlier_remove(outlier_result.get("index"))
                log.debug(f"이상치 제거 후 : {op.get_data().shape}")

        if outlier_save:
            # 이상치제거 파일 저장
            do.set_data(op.get_data())
            do.save_to_csv_file(f"{prop().get_preprocess_file_path()}{media_group_name}_{prop().get_preprocess_result_file_name()}")
