from multipledispatch import dispatch
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import VectorAssembler, StringIndexer, Bucketizer
from pyspark.ml.feature import Normalizer, StandardScaler, RobustScaler, MinMaxScaler, MaxAbsScaler
from src.nurier.ml.common.SparkCommon import SparkCommon as scommon
from src.nurier.ml.data.DataObject import DataObject

import warnings

warnings.filterwarnings("ignore")


class SparkDataObject:
    __data: DataFrame
    __columnList: list
    split_data: list
    pipeline_array: list
    features: VectorAssembler
    label_column: str

    media_group_name: str
    media_group_code_array: list

    def __init__(self, data: DataObject):
        spark = scommon.getInstance().get_spark_session()

        self.__data = spark.createDataFrame(data.get_data())
        self.__columnList = self.__data.columns
        self.media_group_name = data.get_media_group_name()
        self.media_group_code_array = data.get_media_group_code_array()
        self.pipeline_array = []

    # Getter / Setter
    def get_data(self):
        return self.__data

    def set_data(self, data):
        self.__data = data
        self.__columnList = self.__data.columns

    def get_media_group_name(self):
        return self.media_group_name

    def set_media_group_name(self, media_group_name):
        self.media_group_name = media_group_name

    def get_media_group_code_array(self):
        return self.media_group_code_array

    def set_media_group_code_array(self, media_group_code_array):
        self.media_group_code_array = media_group_code_array

    def get_pipeline_array(self):
        return self.pipeline_array

    def set_pipeline_array(self, pipeline_array):
        self.pipeline_array = pipeline_array

    def get_features(self):
        return self.features

    def get_label_column(self):
        return self.label_column

    def set_label_column(self, label_column):
        self.label_column = label_column

    def get_columns_size(self):
        return len(self.__columnList)

    # Data Split
    @dispatch(float)
    def set_split_data(self, train_size):
        splits: list = [train_size, 1-train_size]
        self.split_data = self.__data.randomSplit(splits)

    @dispatch(float, int)
    def set_split_data(self, train_size, seed):
        splits: list = [train_size, 1 - train_size]
        self.split_data = self.__data.randomSplit(weights=splits, seed=seed)

    def get_split_data(self, data_index):
        if self.split_data is None:
            return self.__data
        elif len(self.split_data) <= data_index:
            return self.split_data[len(self.split_data)-1]
        else:
            return self.split_data[data_index]

    def get_train_data(self):
        if self.split_data is None:
            return self.__data
        return self.split_data[0]

    def get_test_data(self):
        if self.split_data is None:
            return self.__data
        return self.split_data[1]

    def set_train_data(self, data):
        self.split_data[0] = data

    def set_test_data(self, data):
        self.split_data[1] = data

    # Features Settings
    @dispatch(str, list)
    def append_features(self, output_column, columns):
        self.pipeline_array.append(VectorAssembler(inputCols=columns, outputCol=output_column, handleInvalid="skip"))

    @dispatch(list)
    def append_features(self, columns):
        self.pipeline_array.append(VectorAssembler(inputCols=columns, outputCol="features", handleInvalid="skip"))

    def set_features(self, columns):
        self.features = VectorAssembler(inputCols=columns, outputCol="features", handleInvalid="skip")

    # Column Drop
    def column_drop(self, columns):
        self.__data = self.__data.drop(columns)
        self.__columnList = self.__data.columns

    # Column Rename
    def column_rename(self, existing_name, new_name):
        self.__data = self.__data.withColumnRenamed(existing_name, new_name)

    # String Indexer
    def append_category_indexer(self, columns):
        for column in columns:
            self.pipeline_array.append(StringIndexer(inputCol=column, outputCol=f"{column}Index", handleInvalid="keep"))

    @dispatch(list)
    def transform_category_indexer(self, columns):
        self.transform_category_indexer(True, columns)

    @dispatch(bool, list)
    def transform_category_indexer(self, is_original_drop, columns):
        for column in columns:
            category_indexer = StringIndexer(inputCol=column, outputCol=f"{column}Index", handleInvalid="keep")
            self.__data = category_indexer.fit(self.__data).transform(self.__data)

            if is_original_drop:
                self.__data = self.__data.drop(column)

    # Bucketizer
    @dispatch(list, list)
    def append_bucketizer(self, columns, splits):
        output_columns = []
        for idx in range(len(columns)):
            output_columns.append(f"{columns.__getitem__(idx)}Bucketed")
        self.pipeline_array.append(Bucketizer(inputCols=columns, outputCols=output_columns, splitsArray=splits))

    @dispatch(str, list)
    def append_bucketizer(self, column, splits):
        self.pipeline_array.append(Bucketizer(inputCol=column, outputCol=f"{column}Bucketed", splits=splits))

    # Normalizer : param 값은 항상 1.0 이상 ( default 2.0 )
    # ex) setNormalizer(True, "features", 1.0, False)
    @dispatch(str, str, float)
    def append_normalizer(self, column, output_column, param):
        self.pipeline_array.append(Normalizer(inputCol=column, outputCol=output_column, p=param))

    @dispatch(str, float)
    def append_normalizer(self, column, param):
        self.pipeline_array.append(Normalizer(inputCol=column, outputCol=f"{column}Normalized", p=param))

    # Standard Scaler : 평균 0 , 분산 1 이 되도록 데이터 조정
    @dispatch(str, str, bool, bool)
    def append_standard_scaler(self, column, output_column, with_std, with_mean):
        self.pipeline_array.append(StandardScaler(inputCol=column, outputCol=output_column, withStd=with_std, withMean=with_mean))

    @dispatch(str, bool, bool)
    def append_standard_scaler(self, column, with_std, with_mean):
        self.pipeline_array.append(StandardScaler(inputCol=column, outputCol=f"{column}Scaled", withStd=with_std, withMean=with_mean))

    @dispatch(list, bool, bool)
    def append_standard_scaler(self, columns, with_std, with_mean):
        for column in columns:
            self.pipeline_array.append(StandardScaler(inputCol=column, outputCol=f"{column}Scaled", withStd=with_std, withMean=with_mean))

    # Robust Scaler : 중간값, 사분위값 사용 ( 이상치 영향 최소화 )
    def append_robust_scaler(self, column, output_column):
        self.pipeline_array.append(RobustScaler(inputCol=column, outputCol=output_column))

    # MinMax Scaler : 데이터 범위값을 0 ~ 1 사이로 조정
    @dispatch(str, str)
    def append_minmax_scaler(self, column, output_column):
        self.pipeline_array.append(MinMaxScaler(inputCol=column, outputCol=output_column))

    @dispatch(str)
    def append_minmax_scaler(self, column):
        self.pipeline_array.append(MinMaxScaler(inputCol=column, outputCol=f"{column}Scaled"))

    @dispatch(str, bool)
    def set_minmax_scaler(self, column, is_rename):
        scaler = MinMaxScaler(inputCol=column, outputCol=f"{column}Scaled")
        self.__data = scaler.fit(self.__data).transform(self.__data)
        if is_rename:
            self.__data = self.__data.drop(column).withColumnRenamed(f"{column}Scaled", column)

    @dispatch(list, str)
    def set_minmax_scaler(self, columns, output_column):
        assembler = VectorAssembler(inputCols=columns, outputCol=output_column, handleInvalid="skip")
        self.__data = assembler.transform(self.__data)
        scaler = MinMaxScaler(inputCol=output_column, outputCol=f"{output_column}Scaled")
        self.__data = scaler.fit(self.__data).transform(self.__data)
        self.__data = self.__data.drop(output_column).withColumnRenamed(f"{output_column}Scaled", output_column)

    # MaxAbs Scaler : 음수와 양수의 대칭분포 유지, 절대값의 최대가 1.0
    @dispatch(str, str)
    def append_maxabs_scaler(self, column, output_column):
        self.pipeline_array.append(MaxAbsScaler(inputCol=column, outputCol=output_column))

    @dispatch(str)
    def append_maxabs_scaler(self, column):
        self.pipeline_array.append(MaxAbsScaler(inputCol=column, outputCol=f"{column}Scaled"))

    @dispatch(str, bool)
    def set_maxabs_scaler(self, column, is_rename):
        scaler = MaxAbsScaler(inputCol=column, outputCol=f"{column}Scaled")
        self.__data = scaler.fit(self.__data).transform(self.__data)

        if is_rename:
            self.__data = self.__data.drop(column).withColumnRenamed(f"{column}Scaled", column)

    @dispatch(list, str)
    def set_maxabs_scaler(self, columns, output_column):
        assembler = VectorAssembler(inputCols=columns, outputCol=output_column, handleInvalid="skip")
        self.__data = assembler.transform(self.__data)
        scaler = MaxAbsScaler(inputCol=output_column, outputCol=f"{output_column}Scaled")
        self.__data = scaler.fit(self.__data).transform(self.__data)
        self.__data = self.__data.drop(output_column).withColumnRenamed(f"{output_column}Scaled", output_column)
