import numpy as np
from pandas import DataFrame

from src.nurier.ml.common.CommonProperties import CommonProperties as prop

import warnings

warnings.filterwarnings("ignore")


class OutlierProcess:

    __data: DataFrame

    def __init__(self, df):
        self.__data = df

    def set_data(self, df):
        self.__data = df

    def get_data(self):
        return self.__data

    # 아웃라이어 탐지
    def detect_outlier(self, column):
        if self.__data[column].isnull().sum() > 0:
            self.__data = self.__data.dropna(axis=0)
        Q1 = np.percentile(self.__data[column], 25)
        me = np.percentile(self.__data[column], 50)
        Q3 = np.percentile(self.__data[column], 75)
        IQR = Q3 - Q1
        outlier_step = prop().get_outlier_rate() * IQR
        outlier_indices = list(self.__data[(self.__data[column] < Q1 - outlier_step) | (self.__data[column] > Q3 + outlier_step)].index)
        outlier_values = set(self.__data.loc[outlier_indices, column].values)

        result = {'q1': Q1, 'q3': Q3, 'median': me, 'outlier': ",".join(map(str, outlier_values)), 'index': outlier_indices}
        return result

    # 아웃라이어 제거
    def outlier_remove(self, outlier_index_list):
        df_temp = self.__data.copy()
        df_temp.drop(outlier_index_list, axis=0, inplace=True)
        self.__data = df_temp

    # 아웃라이어 평균값으로 대체
    def outlier_average(self, column, outlier_index_list):
        df_temp = self.__data.copy()
        df_temp.drop(outlier_index_list, axis=0, inplace=True)

        mean = df_temp[column].mean()
        if "int" in self.__data[column].dtype.name:
            mean = int(mean)
        print(f"mean : {mean}")
        self.__data[column].iloc[outlier_index_list] = mean

        return mean

    # 아웃라이어 최댓값, 최솟값으로 대체
    def outlier_minmax(self, column, outlier_index_list):
        df_temp = self.__data.copy()
        df_temp.drop(outlier_index_list, axis=0, inplace=True)

        max = df_temp[column].max()
        min = df_temp[column].min()

        value_set = set()

        for index in outlier_index_list:
            value = self.__data[column].iloc[index]
            print(f"min : {min}, max : {max}, value : {value}")
            if value >= max:
                self.__data[column].iloc[index] = max
                value_set.add(value)
            elif value <= min:
                self.__data[column].iloc[index] = min
                value_set.add(value)
        return {"min": min, "max": max, "value": value_set}

    # 아웃라이어 입력값으로 대체
    def outlier_replace_value(self, column, outlier_index_list, replace_value):
        df_temp = self.__data.copy()
        df_temp.drop(outlier_index_list, axis=0, inplace=True)

        type_name = self.__data[column].dtype.name
        if replace_value != "":
            if "int" in type_name:
                replace_value = int(replace_value)
            elif "float" in type_name:
                replace_value = float(replace_value)
            self.__data[column].iloc[outlier_index_list] = replace_value
