from src.nurier.fds.common.LoggingHandler import LoggingHandler
from src.nurier.ml.data.DataObject import DataObject
from src.nurier.ml.data.DataStructType import ML_Field
from src.nurier.ml.common.CommonProperties import CommonProperties as prop

if __name__ == '__main__':
    logger = LoggingHandler(f"{prop().get_result_log_file_path()}{prop().get_preprocess_log_file_name()}", "w", "DEBUG")
    log = logger.get_log()

    do = DataObject(prop().get_original_data_file_path(), "20221117_ML_*.csv")
    log.debug(f"Data shape : {do.get_data().shape}")

    ## 전처리
    # 사용하지 않는 변수 삭제 : E/S 컬럼 삭제
    drop_list = ML_Field.get_dropList()
    log.debug(f"Drop columns : {', '.join(drop_list)}")
    log.debug(f"columns drop, new shape : {do.get_data().shape}")
    do.col_drop(drop_list)

    # label 값 확인
    label = ML_Field.get_label()
    result = do.check_value_counts(label)
    log.debug(f"데이터 확인 :\n{result}")

    # label 값 변경 : 사기, 사고, 기사기 -> 1 / 나머지 -> 0
    log.debug("label change FRAUD, OVERLAPFRAUD, ACC -> 1")
    replace_dict = {"FRAUD": 1, "OVERLAPFRAUD": 1, "ACC": 1}
    for key, count in result.items():
        if key.__ne__("FRAUD") and key.__ne__("OVERLAPFRAUD") and key.__ne__("ACC"):
            replace_dict[key] = 0
    do.data_replace(label, replace_dict)
    log.debug(f"label 값 변경 후 데이터 확인 :\n{do.check_value_counts(label)}")

    for media_group in prop().get_preprocess_media_type_group():
        media_group_name = media_group.split(":")[0]
        media_group_code_array = media_group.split(":")[1].split(",")

        log.debug(f"mediaGroupName : {media_group_name}")
        log.debug(f"mediaGroupCodeArray : {', '.join(media_group_code_array)}")

        do.set_media_group_name(media_group_name)
        do.set_media_group_code_array(media_group_code_array)

        drop_columns = ML_Field.get_not_in_columnList(media_group_name)
        med_dsc_drops = []
        for column in do.get_data().columns:
            if column in drop_columns:
                med_dsc_drops.append(column)
        dataset = do.get_data().drop(med_dsc_drops, axis=1)

        condition = []
        for index, value in dataset['EBNK_MED_DSC'].items():
            if media_group_code_array.__contains__(str(value)):
                condition.append(index)
        dataset = dataset.loc[condition]

        log.debug(f"채널 데이터 분리 : {media_group_name} -> {dataset.shape}")
        log.debug(f"columns : {', '.join(dataset.columns)}")

        data = DataObject("", "")
        data.set_media_group_name(media_group_name)
        data.set_media_group_code_array(media_group_code_array)
        data.set_data(dataset)

        # 결측치 처리
        null_list = data.check_null()
        log.debug(f"결측값 확인 :\n{null_list}")

        for column, count in null_list.items():
            data.missing_value_replace(column, True, "")

        log.debug(f"결측치를 기본값으로 변경 :\n{data.check_null()}")

        # 문자열 대문자 통일
        upper_list = ML_Field.get_initList("upper", media_group_name)
        log.debug(f"문자열 대문자 통일 : {', '.join(upper_list)}")
        dataset = data.get_data()
        for col in upper_list:
            dataset[col] = dataset[col].str.upper()
            log.debug(f"\n{dataset[col].value_counts()}")
        data.set_data(dataset)

        # 데이터 타입 변환
        string_type_list = ML_Field.get_list_for_stringtype_columns(media_group_name)
        int_type_list = ML_Field.get_list_for_inttype_columns(media_group_name)
        code_list = ML_Field.get_list_for_code_columns(media_group_name)
        log.debug("데이터 타입 변환 :")

        for column in dataset.columns:
            if column in code_list:
                dataset = dataset.astype({column: "int"}, errors='ignore')

            if column in string_type_list:
                dataset = dataset.astype({column: "str"})
            elif column in int_type_list:
                dataset = dataset.astype({column: "int"})
            else:
                dataset = dataset.astype({column: "object"})
            log.debug(f"\n{dataset[column].value_counts()}")
        data.set_data(dataset)

        # 데이터 변경
        replace_list = ML_Field.get_initList("replace", media_group_name)
        replaces = {"VIRUTAL": "VIRTUAL", "VMware": "VMWare", "-": "", "_": ""}
        for column in replace_list:
            log.debug(f"데이터 변경 전 :\n{data.check_value_counts(column)}")
            data.data_replace(column, replaces)
            log.debug(f"데이터 변경 후 :\n{data.check_value_counts(column)}")

        # 스마트뱅킹 통신사 통합
        if media_group_name.__eq__("SB"):
            locale_check = data.check_value_counts("sm_locale")
            replace_locale = {}
            for value, count in locale_check.items():
                if value.startswith("zh"):
                    replace_locale[value] = "zh"
            data.data_replace("sm_locale", replace_locale)
            telecom_list = ML_Field.get_initList("telecom", media_group_name)
            data.combine_telecom(telecom_list, False)
            telecom_list.append("sm_locale")
            for column in telecom_list:
                log.debug(f"스마트뱅킹 데이터 변환 : \n{data.check_value_counts(column)}")

        # 콤마 변환
        replace_comma = {",": "&"}
        for column in data.get_data().columns:
            data.data_replace(column, replace_comma)

        # 단일 데이터 삭제
        unique_list = data.unique_value_delete()
        log.debug(f"단일 데이터 삭제 :\n{','.join(unique_list)}")

        # data.apply_robust_scaler([], True)
        # data.data_categorier([], True)

        # 전처리 파일 저장
        data.save_to_csv_file(f"{prop().get_preprocess_file_path()}{media_group_name}_{prop().get_preprocess_result_file_name()}")
