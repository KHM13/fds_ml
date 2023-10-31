from enum import Enum
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from src.nurier.ml.common.CommonProperties import CommonProperties as prop

class ML_Field(Enum):
    TR_DTM = ("날짜", "TR_DTM", StringType(), "drop", "", "N", False, False, "IB,SB")
    indexName = ("인덱스명", "indexName", StringType(), "drop", "", "N", False, False, "")
    documentTypeName = ("인덱스명", "documentTypeName", StringType(), "drop", "", "N", False, False, "")
    EBNK_MED_DSC = ("E뱅킹매체구분코드", "EBNK_MED_DSC", StringType(), "feature", "", "00", True, False, "IB,SB")
    COPR_DS = ("기업구분", "COPR_DS", StringType(), "feature", "", "0", True, False, "IB,SB")
    LANG_DS = ("언어구분", "LANG_DS", StringType(), "feature", "", "N", True, False, "IB,SB")
    E_FNC_USR_OS_DSC = ("E금융사용자운영체제구분코드", "E_FNC_USR_OS_DSC", StringType(), "feature", "", "0", True, False, "IB,SB")
    E_FNC_LGIN_DSC = ("E금융로그인구분코드", "E_FNC_LGIN_DSC", StringType(), "feature", "", "0", True, False, "IB,SB")
    NBNK_C = ("출금계좌은행코드", "NBNK_C", StringType(), "feature", "", "0", True, False, "IB,SB")
    RMS_SVC_C = ("수신서비스코드", "RMS_SVC_C", StringType(), "feature", "", "N", True, False, "IB,SB")
    E_FNC_USR_DVIC_INF_CNTN = ("E금융사용자디바이스정보내용", "E_FNC_USR_DVIC_INF_CNTN", StringType(), "feature", "split", "N", True, False, "IB,SB")
    E_FNC_USR_ACS_DSC = ("E금융사용자접근구분코드", "E_FNC_USR_ACS_DSC", StringType(), "feature", "", "0", True, False, "IB,SB")
    E_FNC_MED_SVCID = ("E금융매체서비스ID", "E_FNC_MED_SVCID", StringType(), "drop", "", "N", False, False, "IB,SB")
    E_FNC_MED_SVRNM = ("E금융매체서버명", "E_FNC_MED_SVRNM", StringType(), "drop", "", "N", False, False, "IB,SB")
    E_FNC_RSP_C = ("E금융응답코드", "E_FNC_RSP_C", StringType(), "feature", "", "0", True, False, "IB,SB")
    EXE_YN = ("실행여부", "EXE_YN", StringType(), "drop", "", "N", False, False, "IB,SB")
    Amount = ("금액", "Amount", IntegerType(), "feature", "", "0", False, True, "IB,SB")
    E_FNC_TR_ACNO_C = ("타계좌구분코드", "E_FNC_TR_ACNO_C", StringType(), "feature", "", "0", True, False, "IB,SB")
    IO_EA_DD1_FTR_LMT3 = ("1일이체한도(만원)", "IO_EA_DD1_FTR_LMT3", IntegerType(), "feature", "", "0", False, True, "IB,SB")
    IO_EA_TM1_FTR_LMT3 = ("1회이체한도(만원)", "IO_EA_TM1_FTR_LMT3", IntegerType(), "feature", "", "0", False, True, "IB,SB")
    IO_EA_PW_CD_DS1 = ("보안매체 오류횟수", "IO_EA_PW_CD_DS1", IntegerType(), "feature", "", "0", False, True, "IB,SB")
    IO_EA_PW_CD_DS2 = ("보안매체 종류코드", "IO_EA_PW_CD_DS2", StringType(), "feature", "", "N", True, False, "IB,SB")
    IO_EA_PW_CD_DS3 = ("키락이용여부", "IO_EA_PW_CD_DS3", StringType(), "feature", "", "0", True, False, "IB,SB")
    PRE_ASSIGN_YN = ("단말기지정여부", "PRE_ASSIGN_YN", StringType(), "feature", "", "0", True, False, "IB,SB")
    SMS_AUTHEN_YN = ("휴대폰SMS인증여부", "SMS_AUTHEN_YN", StringType(), "feature", "", "0", True, False, "IB,SB")
    EXCEPT_REGIST = ("예외고객등록여부", "EXCEPT_REGIST", StringType(), "feature", "", "0", True, False, "IB,SB")
    EXCEPTION_ADD_AUTHEN_YN = ("SMS통지가입여부", "EXCEPTION_ADD_AUTHEN_YN", StringType(), "feature", "", "0", True, False, "IB,SB")
    SMART_AUTHEN_YN = ("앱인증서비스", "SMART_AUTHEN_YN", StringType(), "feature", "", "0", True, False, "IB,SB")
    ATTC_DS = ("전화승인서비스 5회 오류", "ATTC_DS", IntegerType(), "feature", "", "0", False, True, "IB,SB")
    FTR_DS2 = ("이체구분", "FTR_DS2", StringType(), "feature", "", "0", True, False, "IB,SB")
    RV_AC_DGN_YN2 = ("입금계좌지정여부", "RV_AC_DGN_YN2", StringType(), "feature", "", "0", True, False, "IB,SB")
    IO_EA_DPZ_PL_IMP_BAC = ("수신지불가능잔액", "IO_EA_DPZ_PL_IMP_BAC", IntegerType(), "feature", "", "0", False, True, "IB,SB")
    IO_EA_TOT_BAC6 = ("출금후 잔액", "IO_EA_TOT_BAC6", IntegerType(), "feature", "", "0", False, True, "IB,SB")
    IO_EA_RMT_FEE1 = ("송금수수료", "IO_EA_RMT_FEE1", IntegerType(), "feature", "", "0", False, True, "IB,SB")
    securityMediaType = ("보안매체구분", "securityMediaType", StringType(), "feature", "", "0", True, False, "IB,SB")
    workType = ("부가서비스구분", "workType", StringType(), "feature", "", "N", True, False, "IB,SB")
    workGbn = ("부가서비스유형", "workGbn", StringType(), "feature", "", "0", True, False, "IB,SB")
    FDS_IDEN = ("인증수단코드", "FDS_IDEN", StringType(), "feature", "", "N", True, False, "IB,SB")
    country = ("국가코드", "country", StringType(), "feature", "replace", "N", True, False, "IB,SB")
    doaddress = ("지역", "doaddress", StringType(), "feature", "replace", "N", True, False, "IB,SB")
    pc_PubIpCntryCd = ("공인 IP 국가코드", "pc_PubIpCntryCd", StringType(), "feature", "", "N", True, False, "IB")
    pc_isProxy = ("Proxy 유무", "pc_isProxy", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_PrxyCntryCd = ("Proxy 국가코드", "pc_PrxyCntryCd", StringType(), "feature", "", "N", True, False, "IB")
    pc_isVpn = ("VPN 유무", "pc_isVpn", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_VpnCntryCd = ("VPN 국가코드", "pc_VpnCntryCd", StringType(), "feature", "", "N", True, False, "IB")
    pc_FORGERY_MAC_YN = ("MAC Adress 통합변조여부", "pc_FORGERY_MAC_YN", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_FORGERY_MAC_ETH0_YN = ("MAC Adress 1 변조여부", "pc_FORGERY_MAC_ETH0_YN", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_FORGERY_MAC_ETH1_YN = ("MAC Adress 2 변조여부", "pc_FORGERY_MAC_ETH1_YN", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_FORGERY_MAC_ETH2_YN = ("MAC Adress 3 변조여부", "pc_FORGERY_MAC_ETH2_YN", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_FORGERY_MAC_ETH3_YN = ("MAC Adress 4 변조여부", "pc_FORGERY_MAC_ETH3_YN", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_FORGERY_MAC_ETH4_YN = ("MAC Adress 5 변조여부", "pc_FORGERY_MAC_ETH4_YN", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_FORGERY_MAC_ETH5_YN = ("MAC Adress 6 변조여부", "pc_FORGERY_MAC_ETH5_YN", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_HdModel = ("HDD MODEL NUMBER", "pc_HdModel", StringType(), "feature", "", "N", True, False, "IB")
    pc_BwVsnCd = ("브라우저 버전코드", "pc_BwVsnCd", StringType(), "feature", "", "N", True, False, "IB")
    pc_isVm = ("가상OS사용여부", "pc_isVm", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_vmName = ("가상OS명", "pc_vmName", StringType(), "feature", "replace", "N", True, False, "IB")
    pc_SCAN_CNT_DETECT = ("악성코드 탐지 건수", "pc_SCAN_CNT_DETECT", IntegerType(), "feature", "", "0", False, True, "IB")
    pc_SCAN_CNT_CURED = ("악성코드 치료 건수", "pc_SCAN_CNT_CURED", IntegerType(), "feature", "", "0", False, True, "IB")
    pc_os = ("운영체제 코드", "pc_os", StringType(), "feature", "", "N", True, False, "IB")
    pc_OsLangCd = ("운영체제 언어팩", "pc_OsLangCd", StringType(), "feature", "", "N", True, False, "IB")
    pc_OsRemoteYn = ("운영체제 원격접속 허용 여부", "pc_OsRemoteYn", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_OS_FIREWALL_CD = ("운영체제 방화벽 설정 코드", "pc_OS_FIREWALL_CD", StringType(), "feature", "", "0", True, False, "IB")
    pc_REMOTE_YN = ("원격환경에서의 구동여부", "pc_REMOTE_YN", StringType(), "feature", "upper", "N", True, False, "IB")
    pc_RemoteProg = ("원격프로그램명", "pc_RemoteProg", StringType(), "feature", "", "N", True, False, "IB")
    pc_RemotePORT = ("원격접속 remote Port", "pc_RemotePORT", StringType(), "feature", "", "0", True, False, "IB")
    pc_remoteInfo1 = ("원격정보1", "pc_remoteInfo1", StringType(), "feature", "", "N", True, False, "IB")
    pc_remoteInfo2 = ("원격정보2", "pc_remoteInfo2", StringType(), "feature", "", "N", True, False, "IB")
    pc_remoteInfo3 = ("원격정보3", "pc_remoteInfo3", StringType(), "feature", "", "N", True, False, "IB")
    pc_remoteInfo4 = ("원격정보4", "pc_remoteInfo4", StringType(), "feature", "", "N", True, False, "IB")
    pc_remoteInfo5 = ("원격정보5", "pc_remoteInfo5", StringType(), "feature", "", "N", True, False, "IB")
    pc_remoteInfo6 = ("원격정보6", "pc_remoteInfo6", StringType(), "feature", "", "N", True, False, "IB")
    pc_remoteInfo7 = ("원격정보7", "pc_remoteInfo7", StringType(), "feature", "", "N", True, False, "IB")
    pc_remoteInfo8 = ("원격정보8", "pc_remoteInfo8", StringType(), "feature", "", "N", True, False, "IB")
    pc_isWinDefender = ("Windows Defender 보안적용 여부", "pc_isWinDefender", StringType(), "feature", "", "0", True, False, "IB")
    pc_isCertMisuse = ("공인인증서 오용검사", "pc_isCertMisuse", StringType(), "feature", "", "0", True, False, "IB")
    sm_deviceModel = ("기기모델명", "sm_deviceModel", StringType(), "feature", "split", "N", True, False, "SB")
    sm_osVersion = ("운영체제버전", "sm_osVersion", StringType(), "feature", "", "N", True, False, "SB")
    sm_service = ("통신사업자", "sm_service", StringType(), "feature", "telecom", "N", True, False, "SB")
    sm_locale = ("핸드폰언어설정", "sm_locale", StringType(), "feature", "replace", "N", True, False, "SB")
    sm_network = ("네트워크상태", "sm_network", StringType(), "feature", "", "N", True, False, "SB")
    sm_jailBreak = ("루팅, 탈옥여부", "sm_jailBreak", StringType(), "feature", "", "0", True, False, "SB")
    sm_roaming = ("로밍여부", "sm_roaming", StringType(), "feature", "upper", "N", True, False, "SB")
    sm_wifiApSsid = ("WiFi AP SID", "sm_wifiApSsid", StringType(), "feature", "", "N", True, False, "SB")
    sm_mobileAPSsid = ("통신사", "sm_mobileAPSsid", StringType(), "feature", "telecom", "N", True, False, "SB")
    blockingType = ("차단유무", "blockingType", StringType(), "drop", "", "N", False, False, "IB,SB")
    totalScore = ("고객최종스코어", "totalScore", IntegerType(), "drop", "", "0", False, False, "IB,SB")
    processState = ("고객대응상태", "processState", IntegerType(), "label", "", "0", False, False, "IB,SB")
    isForeigner = ("외국인 구분필드", "isForeigner", StringType(), "feature", "", "0", True, False, "IB,SB")
    isNewAccount = ("신규입금계좌 구분", "isNewAccount", StringType(), "feature", "", "0", True, False, "IB,SB")
    isNewDevice = ("신규사용기기 구분", "isNewDevice", StringType(), "feature", "", "0", True, False, "IB,SB")
    pc_foresicInfo = ("출생년도", "pc_foresicInfo", IntegerType(), "feature", "", "0", False, True, "IB,SB")
    pc_isWinFirewall = ("성별", "pc_isWinFirewall", StringType(), "feature", "", "0", True, False, "IB,SB")

    def __init__(self, field_name, key, type, used, init_value, default_value, category_indexer, scaler, med_dsc):
        self.field_name = field_name
        self.key = key
        self.type = type
        self.used = used
        self.init_value = init_value
        self.default_value = default_value
        self.category_indexer = category_indexer
        self.scaler = scaler
        self.med_dsc = med_dsc

    def is_init_upper(self):
        return "upper".__eq__(self.init_value)

    def is_init_replace(self):
        return "replace".__eq__(self.init_value)

    def is_init_telecom(self):
        return "telecom".__eq__(self.init_value)

    def is_init_split(self):
        return "split".__eq__(self.init_value)

    @staticmethod
    def get_field(column: str):
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if column.__eq__(key.key):
                return key
        return None

    @staticmethod
    def get_list_for_inttype_columns(med_dsc: str):
        list_for_doubletype_columns = []
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if key.med_dsc.__contains__(med_dsc) & (key.type == IntegerType()) & "drop".__ne__(key.used):
                list_for_doubletype_columns.append(key.key)
        print(f"get_list_for_inttype_columns : {', '.join(list_for_doubletype_columns)}")
        return list_for_doubletype_columns

    @staticmethod
    def get_list_for_stringtype_columns(med_dsc: str):
        list_for_stringtype_columns = []
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if key.med_dsc.__contains__(med_dsc) & (key.type == StringType()) & "drop".__ne__(key.used):
                list_for_stringtype_columns.append(key.key)
        print(f"get_list_stringtype_columns : {', '.join(list_for_stringtype_columns)}")
        return list_for_stringtype_columns

    @staticmethod
    def get_list_for_code_columns(med_dsc: str):
        list_for_code_columns = []
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if key.med_dsc.__contains__(med_dsc) & (key.type == StringType()) & "drop".__ne__(key.used) & "0".__eq__(key.default_value):
                list_for_code_columns.append(key.key)
        print(f"get_list_for_code_columns : {', '.join(list_for_code_columns)}")
        return list_for_code_columns

    @staticmethod
    def get_list_for_code_to_name_columns(media_code):
        list_for_result = []
        med_dsc = ML_Field.get_media_code_name_to_code(media_code)
        keys = ML_Field.get_fields()
        drop_columns = ML_Field.__get_drop_columns(med_dsc)
        for key in keys.__iter__():
            if drop_columns.__contains__(key.key):
                continue
            if key.med_dsc.__contains__(med_dsc) & "drop".__ne__(key.used):
                list_for_result.append(key.key)
        print(f"get_list_for_result - {med_dsc} : {', '.join(list_for_result)}")
        return list_for_result

    @staticmethod
    def get_fields():
        return ML_Field.__members__.values()

    @staticmethod
    def get_nonscaler_featurelist(med_dsc: str):
        result = []
        drop_columns = ML_Field.__get_drop_columns(med_dsc)

        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if key.med_dsc.__contains__(med_dsc) & key.used.__eq__("feature"):
                if drop_columns.__contains__(key.key):
                    continue
                if key.category_indexer:
                    result.append(f"{key.key}Index")
                else:
                    result.append(key.key)
        return result

    @staticmethod
    def get_featureList(med_dsc: str):
        result = []
        drop_columns = ML_Field.__get_drop_columns(med_dsc)

        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if key.used.__eq__("feature") & key.med_dsc.__contains__(med_dsc):
                if drop_columns.__contains__(key.key):
                    continue
                if key.category_indexer:
                    result.append(f"{key.key}Index")
                else:
                    result.append(key.key)
        if len(ML_Field.get_scalerList(med_dsc)) > 0:
            result.append("ScalerFeatures")

        return result

    @staticmethod
    def get_dropList():
        result = []
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if key.used.__eq__("drop"):
                result.append(key.key)
        return result

    @staticmethod
    def get_label() -> str:
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if key.used.__eq__("label"):
                return key.key
        return ""

    @staticmethod
    def get_category_indexerList(med_dsc: str):
        result = []
        drop_columns = ML_Field.__get_drop_columns(med_dsc)

        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if drop_columns.__contains__(key.key):
                continue
            if key.category_indexer & key.med_dsc.__contains__(med_dsc):
                result.append(key.key)
        return result

    @staticmethod
    def get_scalerList(med_dsc: str):
        result = []
        drop_columns = ML_Field.__get_drop_columns(med_dsc)

        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if drop_columns.__contains__(key.key):
                continue
            if key.scaler & key.med_dsc.__contains__(med_dsc):
                result.append(key.key)
        return result

    @staticmethod
    def get_not_in_indexer_and_scalerList(med_dsc: str):
        result = []
        drop_columns = ML_Field.__get_drop_columns(med_dsc)

        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if drop_columns.__contains__(key.key):
                continue
            if not key.category_indexer and not key.scaler and key.med_dsc.__contains__(med_dsc):
                result.append(key.key)
        return result

    @staticmethod
    def get_outlierList(med_dsc: str):
        result = []
        drop_columns = ML_Field.__get_drop_columns(med_dsc)

        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if drop_columns.__contains__(key.key):
                continue
            if key.used.__eq__("feature") and not key.category_indexer and key.med_dsc.__contains__(med_dsc) and not key.used.__eq__("drop"):
                result.append(key.key)
        return result

    @staticmethod
    def get_initList(init_value: str, med_dsc: str):
        result = []
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if key.init_value.__contains__(init_value) & key.med_dsc.__contains__(med_dsc):
                result.append(key.key)
        return result

    @staticmethod
    def get_not_in_columnList(med_dsc: str):
        result = []
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if not key.med_dsc.__contains__(med_dsc):
                result.append(key.key)
        return result

    @staticmethod
    def get_default_value(column_name: str):
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if key.key.__eq__(column_name):
                return key.default_value
        return ""

    @staticmethod
    def get_key_and_name(columns: list):
        result = {}
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            for column in columns:
                if key.key.__eq__(column):
                    result[key.key] = key.field_name
        return result

    @staticmethod
    def get_apply_columnList(med_dsc: str):
        result = []
        drop_columns = ML_Field.__get_drop_columns(med_dsc)

        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            if key.med_dsc.__contains__(med_dsc) and key.used.__contains__("feature") and not drop_columns.__contains__(key.key):
                result.append(key)
        return result

    @staticmethod
    def __get_drop_columns(med_dsc: str) -> list:
        try:
            if "IB".__eq__(med_dsc):
                drop_columns = prop().get_model_training_drop_column_ib_list().split(",")
            elif "SB".__eq__(med_dsc):
                drop_columns = prop().get_model_training_drop_column_sb_list().split(",")
            else:
                drop_columns = prop().get_model_training_drop_column_list().split(",")
            return drop_columns
        except Exception as e:
            print(f"ML_Field __get_drop_columns ERROR : {e}")
            return []

    @staticmethod
    def get_media_code_name_to_code(media_code):
        try:
            msc_dsc = "IB"  # default
            for media_group in prop().get_preprocess_media_type_group():
                media_group_name: str = media_group.split(":")[0]
                media_group_code_array: list = media_group.split(":")[1].split(",")
                if media_group_code_array.__contains__(media_code):
                    msc_dsc = media_group_name
            return msc_dsc
        except Exception as e:
            print(f"ML_Field get_media_code_name_to_code ERROR : {e}")
            return "IB"

    @staticmethod
    def get_schema():
        schema = []
        keys = ML_Field.get_fields()
        for key in keys.__iter__():
            schema.append(StructField(key.key, key.type))
        return StructType(schema)