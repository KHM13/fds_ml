import pandas as pd
import numpy as np

from src.nurier.ml.common.SparkCommon import SparkCommon as scommon
from pyspark.sql import SparkSession
from src.nurier.ml.data.DataStructType import ML_Field



def preprocess(df: pd.DataFrame, db_data: list):
    # preprocess
    for process in db_data:
        df.fillna(0)
        """
        =========================
        0. id
        1. column_name
        2. process_type
        3. work_type
        4. input_value
        5. replace_value
        =========================
        """
        process_type = process[2]
        if process_type == "missing":
            # missing(df, column, work_type, input_value)
            df = process_missing(df, process[1], process[3], process[4])
        elif process_type == "replace":
            # replace(df, column, work_input, replace_input)
            df = fs_replace_value(df, process[1], process[4], process[5])
        elif process_type == "datatype":
            # datatype(df, column, data_type)
            df = data_type_control(df, process[1], process[4])
        elif process_type == "except":
            df = df.drop(process[1], axis=1)

    # spark_df transform / return
    ss: SparkSession = scommon.getInstance().get_spark_session()
    spark_df = ss.createDataFrame(df)

    return spark_df


# 데이터 타입 변경(datatype)
def data_type_control(df, column, data_type):
    print(f"column : {column}, data_type : {data_type}")

    if data_type not in df[column].dtypes.name:
        if data_type in "datetime":
            df[column] = pd.to_datetime(df[column])
        elif data_type in "category":
            df[column] = df[column].factorize()[0]
            df.loc[:, [column]] = df.loc[:, [column]].astype(data_type)
        else:
            df.loc[:, [column]] = df.loc[:, [column]].astype(data_type)

    print(f"{column} type name : {df[column].dtype.name}")
    return df


# 결측치 처리(missing)
def process_missing(df, column, process, input_value):
    df_data: pd.DataFrame = df.copy()
    if process == "remove":     # 결측치 제거
        df_data = df_data.dropna(subset=[column])
    elif process == "interpolation":    # 보간값으로 결측치 변환
        df_data[column] = df_data[column].interpolate(method='values')
    elif process == "mean":     # 평균값으로 결측치 변환
        df_data[column] = df_data[column].fillna(df_data[column].mean())
    elif process == "input":    # 변환값 직접 입력
        type_name = df_data[column].dtypes.name
        print(f" type name : {type_name}, input value : {input_value}")
        if input_value != "":  # 입력값을 str 타입으로 받기 때문에 형변환 필요
            if "int" in type_name:
                input_value = int(input_value)
            elif "float" in type_name:
                input_value = float(input_value)
            df_data[column] = df_data[column].fillna(input_value)
    return df_data


# 문자열 변환(replace)
def fs_replace_value(df, column, work_input, replace_input):
    df_data = df.copy()
    df_data = df_data.replace({column: work_input}, replace_input)
    return df_data
