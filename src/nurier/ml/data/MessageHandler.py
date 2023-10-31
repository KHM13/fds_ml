from src.nurier.ml.event.PredictionEvent import PredictionEvent
from src.nurier.ml.prediction.FDSPrediction import FDSPrediction
from src.nurier.ml.common.SparkCommon import SparkCommon
from src.nurier.ml.data.DataStructType import ML_Field

from pyspark.sql.dataframe import StructType, StringType, IntegerType
from multipledispatch import dispatch


class MessageHandler:

    @staticmethod
    def get_dataframe_from_message(event: PredictionEvent):
        data = MessageHandler.get_row_from_message(event)
        return SparkCommon.getInstance().get_spark_session().createDataFrame([data])

    @staticmethod
    @dispatch(PredictionEvent)
    def get_row_from_message(event: PredictionEvent):
        return MessageHandler.get_row_from_message(FDSPrediction().get_struct_type(event.message.EBNK_MED_DSC), event)

    @staticmethod
    @dispatch(StructType, PredictionEvent)
    def get_row_from_message(struct_type, event):
        row_data = {}
        field_names = ML_Field.get_list_for_code_to_name_columns(event.message.EBNK_MED_DSC)
        if field_names.__contains__("processState"):
            field_names.remove("processState")

        for field in struct_type.fieldNames():
            data = event.message_method_map.get(field)
            ml_field = ML_Field.get_field(field)

            if field_names.__contains__(field):
                if data is not None and data.__ne__(""):
                    if ml_field.type.__eq__(IntegerType()):
                        try:
                            row_data[field] = int(data)
                        except Exception as e:
                            print(f"MessageHandler ERROR : {e}")
                            row_data[field] = 0
                    elif ml_field.type.__eq__(StringType()):
                        data = str(data).replace(",", "&")
                        if ml_field.is_init_upper():
                            row_data[field] = data.upper()
                        elif ml_field.is_init_telecom():
                            row_data[field] = MessageHandler.get_telecom_value(data)
                        elif ml_field.is_init_split():
                            if data is not None and len(data) > 20:
                                row_data[field] = data.split(" ").__getitem__(0)
                            else:
                                row_data[field] = data
                        elif ml_field.is_init_replace():
                            data = data.replace("-", "")
                            data = data.replace("_", "")
                            if field.__eq__("sm_locale") and data.startswith("zh"):
                                data = "zh"
                            row_data[field] = data
                        else:
                            row_data[field] = data
                    else:
                        row_data[field] = str(data)
                else:
                    if ml_field.type.__eq__(IntegerType()):
                        try:
                            row_data[field] = int(ml_field.default_value)
                        except Exception as e:
                            print(f"MessageHandler ERROR : {e}")
                            row_data[field] = 0
                    else:
                        row_data[field] = ml_field.default_value
        return row_data

    @staticmethod
    def get_telecom_value(value):
        if str(value).upper().__contains__("LG"):
            value = "LGT"
        elif str(value).upper().__contains__("KT") or str(value).upper().__contains__("OLLEH"):
            value = "KT"
        elif str(value).upper().__contains__("SK"):
            value = "SKT"
        return value