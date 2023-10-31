import pandas as pd
from pandas import DataFrame
import statsmodels.api as sm
import itertools
import time

from src.nurier.ml.data.DataStructType import ML_Field


class FeatureSelectProcess:

    __data: DataFrame

    def __init__(self, df):
        self.__data = df

    def set_data(self, df):
        self.__data = df

    def get_data(self):
        return self.__data

    def forward_feature_select(self):
        self.__data = sm.add_constant(self.__data, has_constant='add')
        target = ML_Field.get_label()

        x_data = self.__data.drop([target], axis=1)
        y_data = self.__data[target]
        best_model = self.forward_model(x_data, y_data)

        print(f'best model : \n{best_model.summary()}')
        print(f"best AIC : {best_model.aic}")
        print(f"best columns : {[column for column in list(best_model.params.index) if column != 'const']}")

        result = {'aic': best_model.aic, 'columns': [column for column in list(best_model.params.index) if column != 'const'], 'summary': best_model.summary()}
        return result

    def backward_feature_select(self):
        self.__data = sm.add_constant(self.__data, has_constant='add')
        target = ML_Field.get_label()

        x_data = self.__data.drop([target], axis=1)
        y_data = self.__data[target]
        best_model = self.backword_model(x_data, y_data)

        print(f'best model : \n{best_model.summary()}')
        print(f"best AIC : {best_model.aic}")
        print(f"best columns : {[column for column in list(best_model.params.index) if column != 'const']}")

        result = {'aic': best_model.aic, 'columns': [column for column in list(best_model.params.index) if column != 'const'], 'summary': best_model.summary()}
        return result

    def stepwise_feature_select(self):
        self.__data = sm.add_constant(self.__data, has_constant='add')
        target = ML_Field.get_label()

        x_data = self.__data.drop([target], axis=1)
        y_data = self.__data[target]
        best_model = self.stepwise_model(x_data, y_data)

        print(f'best model : \n{best_model.summary()}')
        print(f"best AIC : {best_model.aic}")
        print(f"best columns : {[column for column in list(best_model.params.index) if column != 'const']}")

        result = {'aic': best_model.aic, 'columns': [column for column in list(best_model.params.index) if column != 'const'], 'summary': best_model.summary()}
        return result

    # AIC 구하기
    def processSubset(self, x, y, feature_set):
        model = sm.OLS(y, x[list(feature_set)])  # modeling
        regr = model.fit()  # 모델학습
        AIC = regr.aic  # 모델의 AIC
        return {"model": regr, "AIC": AIC}

    # 전진 선택법(Step=1)
    def forward(self, x, y, predictors):
        # const ( 상수 ) 가 아닌 predictors 에 포함되어있지 않은 변수들 선택
        remaining_predictors = [p for p in x.columns.difference(['const']) if p not in predictors]
        tic = time.time()
        results = []
        for p in remaining_predictors:
            # aic 계산
            results.append(self.processSubset(x=x, y=y, feature_set=predictors + [p] + ['const']))
        # 데이터프레임으로 변환
        models = pd.DataFrame(results)

        # AIC가 가장 낮은 것을 선택
        best_model = models.loc[models['AIC'].argmin()]  # index
        toc = time.time()
        print(f"Processed {models.shape[0]}, models on {len(predictors) + 1}, predictors in {(toc - tic)}")
        print(f"Selected predictors : {best_model['model'].model.exog_names}, AIC : {best_model[0]}")
        return best_model

    # 전진선택법 모델
    def forward_model(self, x, y):
        f_models = pd.DataFrame(columns=["AIC", "model"])
        tic = time.time()
        # 미리 정의된 데이터 변수
        predictors = []
        # 변수1~10개 : 0~9 -> 1~10
        for i in range(1, len(x.columns.difference(['const'])) + 1):
            forward_result = self.forward(x, y, predictors)
            if i > 1:
                if forward_result['AIC'] > fmodel_before:  # 새 조합으로 구한 aic 보다 이전 조합의 aic가 더 낮으면 for문 종료
                    break
            f_models.loc[i] = forward_result
            predictors = f_models.loc[i]["model"].model.exog_names
            fmodel_before = f_models.loc[i]["AIC"]
            predictors = [k for k in predictors if k != 'const']
        toc = time.time()
        print("Total elapesed time : ", (toc - tic), "seconds.")

        return (f_models['model'][len(f_models['model'])])

    # 후진제거법
    def backward(self, x, y, predictors):
        tic = time.time()
        results = []
        # 데이터 변수들이 미리정의된 predictors 조합확인
        for combo in itertools.combinations(predictors, len(predictors) - 1):
            results.append(self.processSubset(x, y, list(combo) + ['const']))
        models = pd.DataFrame(results)
        # 가장 낮은 AIC를 가진 모델을 선택
        best_model = models.loc[models['AIC'].argmin()]
        toc = time.time()
        print("Processed", models.shape[0], "models on", len(predictors) - 1, "predictors in", (toc - tic))
        print("Selected predictors :", best_model['model'].model.exog_names, ' AIC:', best_model[0])
        return best_model

    # 후진 제거법 모델
    def backword_model(self, x, y):
        b_models = pd.DataFrame(columns=["AIC", "model"])
        tic = time.time()
        # 미리 정의된 데이터 변수
        predictors = x.columns.difference(['const'])
        model_before = self.processSubset(x, y, predictors)['AIC']
        while len(predictors) > 1:
            backward_result = self.backward(x, y, predictors)
            if backward_result['AIC'] > model_before:  # 새 조합으로 구한 aic 보다 이전 조합의 aic가 더 낮으면 for문 종료
                break
            b_models.loc[len(predictors) - 1] = backward_result
            predictors = b_models.loc[len(predictors) - 1]["model"].model.exog_names
            model_before = backward_result["AIC"]
            predictors = [k for k in predictors if k != 'const']

        toc = time.time()
        print("Total elapsed time :", (toc - tic), "seconds.")

        return (b_models["model"].dropna().iloc[0])

    # 단계적 선택법
    def stepwise_model(self, x, y):
        step_models = pd.DataFrame(columns=["AIC", "model"])
        tic = time.time()
        predictors = []

        model_before = self.processSubset(x, y, predictors + ['const'])['AIC']
        for i in range(1, len(x.columns.difference(['const'])) + 1):
            forward_result = self.forward(x, y, predictors)
            print("forward")
            step_models.loc[i] = forward_result
            predictors = step_models.loc[i]["model"].model.exog_names
            predictors = [k for k in predictors if k != 'const']
            backword_result = self.backward(x, y, predictors)

            if backword_result['AIC'] < forward_result['AIC']:
                step_models.loc[i] = backword_result
                predictors = step_models.loc[i]["model"].model.exog_names
                model_before = step_models.loc[i]["AIC"]
                predictors = [k for k in predictors if k != 'const']
                print('backward')

            if step_models.loc[i]["AIC"] > model_before:
                break
            else:
                model_before = step_models.loc[i]["AIC"]
        toc = time.time()
        print("Total elapsed time : ", (toc - tic), "seconds")

        return (step_models['model'][len(step_models['model'])])
