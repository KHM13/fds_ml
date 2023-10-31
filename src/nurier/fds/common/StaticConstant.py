from src.nurier.ml.common.CommonProperties import CommonProperties as prop


class StaticConstant:
    SERVER = ""
    SERVER_IS_START = False
    SERVER_SYNCHRONIZED_FLAG = False

    SERVER_HOST_CODE_1 = "0"
    SERVER_HOST_CODE_2 = "5"

    SERVER_MEMBER_NAME_01 = "OEP01"
    SERVER_MEMBER_NAME_02 = "OEP02"
    SERVER_MEMBER_NAME_51 = "OEP51"
    SERVER_MEMBER_NAME_52 = "OEP52"

    CACHE_NAME_LOGIN = "inbound"
    CACHE_NAME_INTRANSFER = "intransfer"
    CACHE_NAME_TransferCommint = "transferCommit"
    CACHE_NAME_INSEARCH = "insearch"
    CACHE_NAME_OneWay = "oneway"

    RuleProcessor_Login_Common = "RuleProcessor_Login_Common"
    RuleProcessor_Login_eBank = "RuleProcessor_Login_eBank"
    RuleProcessor_Login_Smart = "RuleProcessor_Login_Smart"
    RuleProcessor_Login_Tele = "RuleProcessor_Login_Tele"
    RuleProcessor_Transfer_Common = "RuleProcessor_Transfer_Common"
    RuleProcessor_Transfer_eBank = "RuleProcessor_Transfer_eBank"
    RuleProcessor_Transfer_Smart = "RuleProcessor_Transfer_Smart"
    RuleProcessor_Transfer_Tele = "RuleProcessor_Transfer_Tele"
    RuleProcessor_Information = "RuleProcessor_Information"

    LOG_INDEX_NAME = prop().get_search_engine_index_name()
    LOG_MESSAGE_NAME = prop().get_search_engine_index_type_message()

    WORKGBN_SCOREINITIALIZE = "0"
    WORKGBN_TRANSFERCOMMONIT = "1"

    FDS_IDEN_Y = "Y"
    FDS_IDEN_N = "N"

    WORKTYPE_CERT = "CERT"
    WORKTYPE_1 = "1"
    WORKTYPE_2 = "2"
    WORKTYPE_3 = "3"
    WORKTYPE_4 = "4"
    WORKTYPE_5 = "5"
    WORKGBN_1 = "1"
    WORKGBN_2 = "2"
    WORKGBN_3 = "3"
    WORKGBN_4 = "4"
    WORKGBN_5 = "5"
    WORKGBN_6 = "6"
    WORKGBN_7 = "7"
    WORKGBN_8 = "8"
    WORKGBN_9 = "9"

    LOG_TYPE_REALTIME = "REALTIME"
    LOG_TYPE_NOMESSAGE = "NOMESSAGE"
    LOG_TYPE_TEMP = "TEMP"
    LOG_TYPE_ONEWAY = "ONEWAY"

    SECURITYCARDKEY_2 = "IO_EA_PW_CD_DS2"
    SECURITYCARDKEY_3 = "IO_EA_PW_CD_DS3"
    SECURITYCARDKEY_TYPE_1 = "1"
    SECURITYCARDKEY_TYPE_2 = "2"
    SECURITYCARDKEY_TYPE_3 = "3"
    SECURITYCARDKEY_TYPE_4 = "4"
    SECURITYCARDKEY_TYPE_5 = "5"
    SECURITYCARDKEY_TYPE_ETC = "0"

    BLACKRESULT_DEFAULT = "P"
    BLACKRESULT_EXCEPT = "W"
    BLACKRESULT_RANK_0 = "N"
    BLACKRESULT_RANK_1 = "P"
    BLACKRESULT_RANK_2 = "M"
    BLACKRESULT_RANK_3 = "C"
    BLACKRESULT_RANK_4 = "B"
    BLACKRESULT_RANK_4_DATE = "Date_B"
    BLACKRESULT_RANK_3_DATE = "Date_C"
    BLACKRESULT_EXCEPTION_MESSAGE = "2차 차단 방지 (통과 적용)"

    FDSRESULT_DEFAULT = "0"
    FDSRESULT_EXCEPT = "999"
    FDSRESULT_RANK_1 = "0"
    FDSRESULT_RANK_2 = "1"
    FDSRESULT_RANK_3 = "2"
    FDSRESULT_RANK_4 = "3"
    FDSRESULT_RANK_5 = "4"

    ANALYSIS_DETECT_EBANK_MEDIA_CODE = []
    ANALYSIS_DETECT_SMARK_MEDIA_CODE = []
    ANALYSIS_DETECT_MEDIA_CODE = []

    FDSRESULT_ANALYSIS = "date_Analysis"
    FDSRESULT_ANALYSIS_CNT = "device_analysis_cnt"
    FDSRESULT_ANALYSIS_INDEX = "device_analysis_index"
    FDSRESULT_ANALYSIS_LEVEL1 = "device_analysis_level1"
    FDSRESULT_ANALYSIS_EVENT_RESPONSEOBJECT = "ResponseObject"
    FDSRESULT_ANALYSIS_EVENT_BLOCKINGOBJECT = "BlockingObject"

    FDSRESULT_EXCEPTION_MESSAGE = "당일 2차탐지 방지 (통과 적용)"

    BooleanString_0 = "0"
    BooleanString_1 = "1"

    TEL_ACCOUNT_CHAR = "T_"
    TEL_ACCOUNT_CODE_LIST = []

    BLACKRESULT_MAP: dict = {}
    FDSRESULT_MAP: dict = {}

    DEVICE_TYPE_KTB = "KTB"
    DEVICE_TYPE_ASTX = "ASTX"
    DEVICE_TYPE_SMART = "SMART"
    DEVICE_TYPE_NOT = "NOT"

    def __init__(self):
        self.BLACKRESULT_MAP[self.BLACKRESULT_RANK_0] = int(self.FDSRESULT_RANK_1)
        self.BLACKRESULT_MAP[self.BLACKRESULT_RANK_1] = int(self.FDSRESULT_RANK_2)
        self.BLACKRESULT_MAP[self.BLACKRESULT_RANK_2] = int(self.FDSRESULT_RANK_3)
        self.BLACKRESULT_MAP[self.BLACKRESULT_RANK_3] = int(self.FDSRESULT_RANK_4)
        self.BLACKRESULT_MAP[self.BLACKRESULT_RANK_4] = int(self.FDSRESULT_RANK_5)
        self.BLACKRESULT_MAP[self.BLACKRESULT_EXCEPT] = int(self.FDSRESULT_EXCEPT)

        self.FDSRESULT_MAP[self.FDSRESULT_RANK_1] = int(self.FDSRESULT_RANK_1)
        self.FDSRESULT_MAP[self.FDSRESULT_RANK_2] = int(self.FDSRESULT_RANK_2)
        self.FDSRESULT_MAP[self.FDSRESULT_RANK_3] = int(self.FDSRESULT_RANK_3)
        self.FDSRESULT_MAP[self.FDSRESULT_RANK_4] = int(self.FDSRESULT_RANK_4)
        self.FDSRESULT_MAP[self.FDSRESULT_RANK_5] = int(self.FDSRESULT_RANK_5)

        self.TEL_ACCOUNT_CODE_LIST.append("A1")

        self.SERVER_IS_START = True
