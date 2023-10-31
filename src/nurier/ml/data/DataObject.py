import os
import glob
import pandas as pd
import numpy as np
from pandas import DataFrame
from multipledispatch import dispatch
from imblearn.over_sampling import SMOTE, RandomOverSampler
from imblearn.under_sampling import RandomUnderSampler
from imblearn.pipeline import Pipeline
from pyspark.sql.types import StringType, DoubleType
from sklearn.preprocessing import MinMaxScaler, RobustScaler, StandardScaler
from sklearn.model_selection import train_test_split, RepeatedStratifiedKFold, cross_val_score
from src.nurier.ml.data.DataStructType import ML_Field


class DataObject:

    __data: DataFrame
    __train_data: DataFrame
    __test_data: DataFrame
    media_group_name: str
    media_group_code_array: list

    @dispatch(str, str)
    def __init__(self, file_path: str, file_name: str):
        if file_path.__ne__("") and file_name.__ne__(""):
            files = os.path.join(file_path, file_name)
            list_files = glob.glob(files)
            data = []
            for file in list_files:
                df = pd.read_csv(file, encoding="euc-kr", index_col=None, header=0)
                data.append(df)
            self.__data = pd.concat(data, axis=0, ignore_index=True)

    @dispatch(str)
    def __init__(self, file_name: str):
        if file_name is not None:
            df = pd.read_csv(file_name, encoding="euc-kr", index_col=None, header=0)
            self.__data = pd.DataFrame(df)

    def get_data(self):
        return self.__data

    def set_data(self, data: DataFrame):
        self.__data = data

    def get_training_data(self):
        return self.__train_data

    def set_training_data(self, data: DataFrame):
        self.__train_data = data

    def get_test_data(self):
        return self.__test_data

    def set_test_data(self, data: DataFrame):
        self.__test_data = data

    def get_media_group_name(self):
        return self.media_group_name

    def set_media_group_name(self, media_group_name):
        self.media_group_name = media_group_name

    def get_media_group_code_array(self):
        return self.media_group_code_array

    def set_media_group_code_array(self, media_group_code_array):
        self.media_group_code_array = media_group_code_array

    # Dataframe 기본정보 확인
    def data_info(self):
        if self.__data is not None:
            df_info = {"count": self.__data.shape[0], "colcount": self.__data.shape[1], "columns": list(self.__data.columns)}
            return df_info
        else:
            return {"count": 0, "colcount": (0, 0), "columns": []}

    # column 삭제
    def col_drop(self, colNames: list):
        self.__data = self.__data.drop(colNames, axis=1)
        print(f"columns drop, new shape : {self.__data.shape}")

    # column 명 변경
    def col_rename(self, existing_name: str, new_name: str):
        self.__data.rename(columns={existing_name: new_name}, inplace=True)

    # Data 확인
    def check_value_counts(self, column_name: str):
        result = self.__data[column_name].value_counts()
        print(result)
        return result

    # 값 변경
    def data_replace(self, column_name: str, replace_dict: dict):
        replaces = {column_name: replace_dict}
        self.__data = self.__data.replace(replaces)

    # null 값 확인
    def check_null(self):
        null_list = self.__data.isnull().sum()
        print(null_list)
        return null_list

    # 결측치 삭제
    def missing_value_delete(self, column_name: str):
        self.__replace_none(column_name)
        self.__data = self.__data.dropna(subset=[column_name])

    # 결측치를 평균값으로 대체
    def missing_value_average(self, column_name: str):
        self.__replace_none(column_name)
        self.__data[column_name] = self.__data[column_name].fillna(self.__data[column_name].mean())

    # 결측치를 보간값으로 대체
    def missing_value_interpolation(self, column_name: str):
        self.__replace_none(column_name)
        self.__data[column_name] = self.__data[column_name].interpolate(method='values')

    # 결측치를 새로운 값으로 변경
    def missing_value_replace(self, column_name: str, is_default: bool, replace_value):
        self.__replace_none(column_name)
        type_name = ML_Field.get_field(column_name).type
        value = ML_Field.get_default_value(column_name) if is_default else replace_value

        if StringType == type_name:
            self.__data[column_name] = self.__data[column_name].fillna(str(value))
        elif DoubleType == type_name:
            self.__data[column_name] = self.__data[column_name].fillna(float(value))
        else:
            self.__data[column_name] = self.__data[column_name].fillna(value)

    def __replace_none(self, column_name: str):
        self.__data[column_name].replace('', np.nan, inplace=True)
        self.__data[column_name].replace('NULL', np.nan, inplace=True)
        self.__data[column_name].replace('Null', np.nan, inplace=True)
        self.__data[column_name].replace('null', np.nan, inplace=True)

    # 데이터 타입 변경
    def replace_data_type(self, column_name: str, type_name: str):
        self.__data[column_name] = self.__data[column_name].astype(type_name)

    # 컬럼 단일값 삭제 ( 컬럼 내 데이터가 모든 같은 값일 경우 )
    def unique_value_delete(self):
        drop_list = []
        for column in self.__data.columns:
            if len(self.__data[column].unique()) == 1:
                drop_list.append(column)
        self.col_drop(drop_list)
        return drop_list

    def apply_standard_scaler(self, column_list: list, is_scaler: bool):
        columns = ML_Field.get_scalerList(self.media_group_name) if is_scaler else column_list
        for column in columns:
            self.__data[column] = StandardScaler().fit_transform(self.__data[column].values.reshape(-1, 1))

    def apply_robust_scaler(self, column_list: list, is_scaler: bool):
        columns = ML_Field.get_scalerList(self.media_group_name) if is_scaler else column_list
        for column in columns:
            self.__data[column] = RobustScaler().fit_transform(self.__data[column].values.reshape(-1, 1))

    def apply_minmax_scaler(self, column_list: list, is_scaler: bool):
        columns = ML_Field.get_scalerList(self.media_group_name) if is_scaler else column_list
        for column in columns:
            self.__data[column] = MinMaxScaler().fit_transform(self.__data[column].values.reshape(-1, 1))

    def data_categorier(self, column_list: list, is_category: bool):
        columns = ML_Field.get_category_indexerList(self.media_group_name) if is_category else column_list
        for column in columns:
            self.__data[column] = self.__data[column].factorize()[0]

    def combine_telecom(self, column_list: list, is_telecom: bool):
        columns = ML_Field.get_initList("telecom", self.media_group_name) if is_telecom else column_list
        for column in columns:
            replace_dict = {}
            checked_data = self.__data[column].value_counts()
            for value, count in checked_data.items():
                if str(value).upper().__contains__("LG"):
                    replace_dict[value] = "LGT"
                elif str(value).upper().__contains__("KT") or str(value).upper().__contains__("OLLEH"):
                    replace_dict[value] = "KT"
                elif str(value).upper().__contains__("SK"):
                    replace_dict[value] = "SKT"
            self.data_replace(column, replace_dict)
            replace_dict.clear()

    def train_test_data_division(self, train_size):
        feature_columns = ML_Field.get_featureList(self.media_group_name)
        drop_columns = [column for column in list(self.__data.columns) if column not in feature_columns]
        print(f"drop_columns : {drop_columns}")

        target = ML_Field.get_label()
        x_data = self.__data.drop(drop_columns, axis=1)
        y_data = self.__data[[target]]
        x_train, x_test, y_train, y_test = train_test_split(x_data, y_data, train_size=train_size, random_state=42)
        print(f"train : x - {x_train.shape}, y - {y_train.shape} | test : x - {x_test.shape}, y - {y_test.shape}")

        self.__train_data = pd.concat([x_train, y_train], axis=1)
        self.__test_data = pd.concat([x_test, y_test], axis=1)

        return x_train, x_test, y_train, y_test

    @staticmethod
    def apply_under_sampling(train_data):
        target = ML_Field.get_label()
        f_target = len(train_data.loc[train_data[target] == 1])
        train_data = train_data.sample(frac=1)

        fraud_data = train_data.loc[train_data[target] == 1]
        non_fraud_data = train_data.loc[train_data[target] == 0][:f_target]

        normal_distributed_data = pd.concat([fraud_data, non_fraud_data])
        nd_data = normal_distributed_data.sample(frac=1, random_state=42)

        return nd_data

    @staticmethod
    def apply_over_sampling(train_X, train_y):
        smote = SMOTE(random_state=42)
        smote_X, smote_y = smote.fit(train_X, train_y)

        smote_concat_data = pd.concat([smote_X, smote_y])
        smote_data = smote_concat_data.sample(frac=1, random_state=42)

        return smote_data

    @staticmethod
    def apply_hybrid_resampling(train_X, train_y, model):
        over = RandomOverSampler(sampling_strategy=0.1)
        under = RandomUnderSampler(sampling_strategy=0.5)
        pipeline = Pipeline(steps=[('o', over), ('u', under), ('m', model)])
        cv = RepeatedStratifiedKFold(n_splits=10, n_repeats=3, random_state=1)
        scores = cross_val_score(pipeline, train_X, train_y, scoring='roc_auc', cv=cv, n_jobs=-1)
        print("Mean ROC AUC : %.3f" % np.mean(scores))

    def save_to_csv_file(self, file_path: str):
        self.__data.to_csv(file_path, sep=",", mode="w", encoding="EUC-KR", index=False, header=True)
