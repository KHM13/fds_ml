from json import loads
from src.nurier.ml.event.PredictionEvent import PredictionEvent
from src.nurier.ml.data.MessageHandler import MessageHandler
from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.fds.common.StaticCommonUtil import StaticCommonUtil as scutil
from src.nurier.ml.common.SparkCommon import SparkCommon as scommon
from src.nurier.fds.common.Message import Message
from pyspark.sql.session import SparkSession
import threading


class KafkaSparkListener:

    topic_name: str
    group_id: str
    server: list
    loop = None
    is_loop: bool = True

    def __init__(self):
        self.topic_name = prop().get_listener_engine_kafka_ml_topic_name()
        self.group_id = prop().get_listener_engine_kafka_ml_group_id()
        self.server = prop().get_listener_engine_ml_servers()
        self.spark: SparkSession = scommon.getInstance().get_spark_session()

    def start_listener(self, ib_schedule, sb_schedule):
        self._start_consumer(ib_schedule, sb_schedule)

    def _start_consumer(self, ib_schedule, sb_schedule):
        try:
            df = self.spark.read.format("kafka") \
                .option("kafka.bootstrap.servers", str(self.server)) \
                .option("subscribe", self.topic_name) \
                .option("startingOffsets", "earliest") \
                .load()
            threading.Timer(interval=1.0, function=self._start_consumer, args=(df, ib_schedule, sb_schedule)).start()
        except Exception as e:
            print(f"_start_consumer [ERROR] : {e}")

    def setting_event(self, value, ib_schedule, sb_schedule):
        message = value.toJSON()
        event = PredictionEvent(self.set_message_value(message))
        event.set_original_message(message)
        # row_data = MessageHandler.get_dataframe_from_message(event)
        event.row_message = value
        if ib_schedule.prediction_model.model_IB.is_use_media_code(event.message.EBNK_MED_DSC):
            ib_schedule.use_data_IB(True, event)
        elif sb_schedule.prediction_model.model_SB.is_use_media_code(event.message.EBNK_MED_DSC):
            sb_schedule.use_data_SB(True, event)
        else:
            ib_schedule.use_data_IB(True, event)
        print("setting_event done")

    def set_message_value(self, consume_value: dict):
        message = Message()
        try:
            message.TR_DTM = consume_value.get("TR_DTM") if consume_value.get("TR_DTM") is not None else ""
            message.EBNK_MED_DSC = consume_value.get("EBNK_MED_DSC") if consume_value.get("EBNK_MED_DSC") is not None else ""
            message.COPR_DS = consume_value.get("COPR_DS") if consume_value.get("COPR_DS") is not None else ""
            message.LANG_DS = consume_value.get("LANG_DS") if consume_value.get("LANG_DS") is not None else ""
            message.NBNK_C = consume_value.get("NBNK_C") if consume_value.get("NBNK_C") is not None else ""
            message.E_FNC_USR_OS_DSC = consume_value.get("E_FNC_USR_OS_DSC") if consume_value.get("E_FNC_USR_OS_DSC") is not None else ""
            message.E_FNC_LGIN_DSC = consume_value.get("E_FNC_LGIN_DSC") if consume_value.get("E_FNC_LGIN_DSC") is not None else ""
            message.RMS_SVC_C = consume_value.get("RMS_SVC_C") if consume_value.get("RMS_SVC_C") is not None else ""
            message.E_FNC_USR_DVIC_INF_CNTN = consume_value.get("E_FNC_USR_DVIC_INF_CNTN") if consume_value.get("E_FNC_USR_DVIC_INF_CNTN") is not None else ""
            message.E_FNC_USR_ACS_DSC = consume_value.get("E_FNC_USR_ACS_DSC") if consume_value.get("E_FNC_USR_ACS_DSC") is not None else ""
            message.E_FNC_MED_SVCID = consume_value.get("E_FNC_MED_SVCID") if consume_value.get("E_FNC_MED_SVCID") is not None else ""
            message.E_FNC_MED_SVRNM = consume_value.get("E_FNC_MED_SVRNM") if consume_value.get("E_FNC_MED_SVRNM") is not None else ""
            message.E_FNC_RSP_C = consume_value.get("E_FNC_RSP_C") if consume_value.get("E_FNC_RSP_C") is not None else ""
            message.EXE_YN = consume_value.get("EXE_YN") if consume_value.get("EXE_YN") is not None else ""
            message.E_FNC_TR_ACNO_C = consume_value.get("E_FNC_TR_ACNO_C") if consume_value.get("E_FNC_TR_ACNO_C") is not None else ""
            message.IO_EA_PW_CD_DS1 = consume_value.get("IO_EA_PW_CD_DS1") if consume_value.get("IO_EA_PW_CD_DS1") is not None else ""
            message.IO_EA_PW_CD_DS2 = consume_value.get("IO_EA_PW_CD_DS2") if consume_value.get("IO_EA_PW_CD_DS2") is not None else ""
            message.IO_EA_PW_CD_DS3 = consume_value.get("IO_EA_PW_CD_DS3") if consume_value.get("IO_EA_PW_CD_DS3") is not None else ""
            message.PRE_ASSIGN_YN = consume_value.get("PRE_ASSIGN_YN") if consume_value.get("PRE_ASSIGN_YN") is not None else ""
            message.SMS_AUTHEN_YN = consume_value.get("SMS_AUTHEN_YN") if consume_value.get("SMS_AUTHEN_YN") is not None else ""
            message.EXCEPT_REGIST = consume_value.get("EXCEPT_REGIST") if consume_value.get("EXCEPT_REGIST") is not None else ""
            message.EXCEPTION_ADD_AUTHEN_YN = consume_value.get("EXCEPTION_ADD_AUTHEN_YN") if consume_value.get("EXCEPTION_ADD_AUTHEN_YN") is not None else ""
            message.SMART_AUTHEN_YN = consume_value.get("SMART_AUTHEN_YN") if consume_value.get("SMART_AUTHEN_YN") is not None else ""
            message.ATTC_DS = consume_value.get("ATTC_DS") if consume_value.get("ATTC_DS") is not None else ""
            message.FTR_DS2 = consume_value.get("FTR_DS2") if consume_value.get("FTR_DS2") is not None else ""
            message.RV_AC_DGN_YN2 = consume_value.get("RV_AC_DGN_YN2") if consume_value.get("RV_AC_DGN_YN2") is not None else ""

            if consume_value.get("Amount") is not None and scutil().isNumberic(consume_value.get("Amount")):
                message.Amount = float(consume_value.get("Amount"))
            else:
                message.Amount = 0.0

            if consume_value.get("IO_EA_RMT_FEE1") is not None and scutil().isNumberic(consume_value.get("IO_EA_RMT_FEE1")):
                message.IO_EA_RMT_FEE1 = float(consume_value.get("IO_EA_RMT_FEE1"))
            else:
                message.IO_EA_RMT_FEE1 = 0.0

            if consume_value.get("IO_EA_DD1_FTR_LMT3") is not None and scutil().isNumberic(consume_value.get("IO_EA_DD1_FTR_LMT3")):
                message.IO_EA_DD1_FTR_LMT3 = float(consume_value.get("IO_EA_DD1_FTR_LMT3"))
            else:
                message.IO_EA_DD1_FTR_LMT3 = 0.0

            if consume_value.get("IO_EA_TM1_FTR_LMT3") is not None and scutil().isNumberic(consume_value.get("IO_EA_TM1_FTR_LMT3")):
                message.IO_EA_TM1_FTR_LMT3 = float(consume_value.get("IO_EA_TM1_FTR_LMT3"))
            else:
                message.IO_EA_TM1_FTR_LMT3 = 0.0

            if consume_value.get("IO_EA_DPZ_PL_IMP_BAC") is not None and scutil().isNumberic(consume_value.get("IO_EA_DPZ_PL_IMP_BAC")):
                message.IO_EA_DPZ_PL_IMP_BAC = float(consume_value.get("IO_EA_DPZ_PL_IMP_BAC"))
            else:
                message.IO_EA_DPZ_PL_IMP_BAC = 0.0

            if consume_value.get("IO_EA_TOT_BAC6") is not None and scutil().isNumberic(consume_value.get("IO_EA_TOT_BAC6")):
                message.IO_EA_TOT_BAC6 = float(consume_value.get("IO_EA_TOT_BAC6"))
            else:
                message.IO_EA_TOT_BAC6 = 0.0

            message.totalScore = float(consume_value.get("totalScore")) if consume_value.get("totalScore") is not None else 0.0
            message.pc_isProxy = consume_value.get("pc_isProxy") if consume_value.get("pc_isProxy") is not None else ""
            message.pc_isVpn = consume_value.get("pc_isVpn") if consume_value.get("pc_isVpn") is not None else ""
            message.pc_os = consume_value.get("pc_os") if consume_value.get("pc_os") is not None else ""
            message.pc_OsLangCd = consume_value.get("pc_OsLangCd") if consume_value.get("pc_OsLangCd") is not None else ""
            message.pc_OsRemoteYn = consume_value.get("pc_OsRemoteYn") if consume_value.get("pc_OsRemoteYn") is not None else ""
            message.pc_OS_FIREWALL_CD = consume_value.get("pc_OS_FIREWALL_CD") if consume_value.get("pc_OS_FIREWALL_CD") is not None else ""
            message.pc_isVm = consume_value.get("pc_isVm") if consume_value.get("pc_isVm") is not None else ""
            message.pc_vmName = consume_value.get("pc_vmName") if consume_value.get("pc_vmName") is not None else ""
            message.pc_REMOTE_YN = consume_value.get("pc_REMOTE_YN") if consume_value.get("pc_REMOTE_YN") is not None else ""
            message.pc_RemoteProg = consume_value.get("pc_RemoteProg") if consume_value.get("pc_RemoteProg") is not None else ""
            message.pc_RemotePORT = consume_value.get("pc_RemotePORT") if consume_value.get("pc_RemotePORT") is not None else ""
            message.pc_remoteInfo1 = consume_value.get("pc_remoteInfo1") if consume_value.get("pc_remoteInfo1") is not None else ""
            message.pc_remoteInfo2 = consume_value.get("pc_remoteInfo2") if consume_value.get("pc_remoteInfo2") is not None else ""
            message.pc_remoteInfo3 = consume_value.get("pc_remoteInfo3") if consume_value.get("pc_remoteInfo3") is not None else ""
            message.pc_remoteInfo4 = consume_value.get("pc_remoteInfo4") if consume_value.get("pc_remoteInfo4") is not None else ""
            message.pc_remoteInfo5 = consume_value.get("pc_remoteInfo5") if consume_value.get("pc_remoteInfo5") is not None else ""
            message.pc_remoteInfo6 = consume_value.get("pc_remoteInfo6") if consume_value.get("pc_remoteInfo6") is not None else ""
            message.pc_remoteInfo7 = consume_value.get("pc_remoteInfo7") if consume_value.get("pc_remoteInfo7") is not None else ""
            message.pc_remoteInfo8 = consume_value.get("pc_remoteInfo8") if consume_value.get("pc_remoteInfo8") is not None else ""
            message.pc_foresicInfo = consume_value.get("pc_foresicInfo") if consume_value.get("pc_foresicInfo") is not None else ""
            message.pc_isWinDefender = consume_value.get("pc_isWinDefender") if consume_value.get("pc_isWinDefender") is not None else ""
            message.pc_isWinFirewall = consume_value.get("pc_isWinFirewall") if consume_value.get("pc_isWinFirewall") is not None else ""
            message.pc_isCertMisuse = consume_value.get("pc_isCertMisuse") if consume_value.get("pc_isCertMisuse") is not None else ""
            message.pc_PubIpCntryCd = consume_value.get("pc_PubIpCntryCd") if consume_value.get("pc_PubIpCntryCd") is not None else ""
            message.pc_PrxyCntryCd = consume_value.get("pc_PrxyCntryCd") if consume_value.get("pc_PrxyCntryCd") is not None else ""
            message.pc_VpnCntryCd = consume_value.get("pc_VpnCntryCd") if consume_value.get("pc_VpnCntryCd") is not None else ""
            message.pc_FORGERY_MAC_YN = consume_value.get("pc_FORGERY_MAC_YN") if consume_value.get("pc_FORGERY_MAC_YN") is not None else ""
            message.pc_FORGERY_MAC_ETH0_YN = consume_value.get("pc_FORGERY_MAC_ETH0_YN") if consume_value.get("pc_FORGERY_MAC_ETH0_YN") is not None else ""
            message.pc_FORGERY_MAC_ETH1_YN = consume_value.get("pc_FORGERY_MAC_ETH1_YN") if consume_value.get("pc_FORGERY_MAC_ETH1_YN") is not None else ""
            message.pc_FORGERY_MAC_ETH2_YN = consume_value.get("pc_FORGERY_MAC_ETH2_YN") if consume_value.get("pc_FORGERY_MAC_ETH2_YN") is not None else ""
            message.pc_FORGERY_MAC_ETH3_YN = consume_value.get("pc_FORGERY_MAC_ETH3_YN") if consume_value.get("pc_FORGERY_MAC_ETH3_YN") is not None else ""
            message.pc_FORGERY_MAC_ETH4_YN = consume_value.get("pc_FORGERY_MAC_ETH4_YN") if consume_value.get("pc_FORGERY_MAC_ETH4_YN") is not None else ""
            message.pc_FORGERY_MAC_ETH5_YN = consume_value.get("pc_FORGERY_MAC_ETH5_YN") if consume_value.get("pc_FORGERY_MAC_ETH5_YN") is not None else ""
            message.pc_HdModel = consume_value.get("pc_HdModel") if consume_value.get("pc_HdModel") is not None else ""
            message.pc_BwVsnCd = consume_value.get("pc_BwVsnCd") if consume_value.get("pc_BwVsnCd") is not None else ""
            message.pc_SCAN_CNT_DETECT = consume_value.get("pc_SCAN_CNT_DETECT") if consume_value.get("pc_SCAN_CNT_DETECT") is not None else ""
            message.pc_SCAN_CNT_CURED = consume_value.get("pc_SCAN_CNT_CURED") if consume_value.get("pc_SCAN_CNT_CURED") is not None else ""

            message.sm_deviceModel = consume_value.get("sm_deviceModel") if consume_value.get("sm_deviceModel") is not None else ""
            message.sm_osVersion = consume_value.get("sm_osVersion") if consume_value.get("sm_osVersion") is not None else ""
            message.sm_service = consume_value.get("sm_service") if consume_value.get("sm_service") is not None else ""
            message.sm_locale = consume_value.get("sm_locale") if consume_value.get("sm_locale") is not None else ""
            message.sm_network = consume_value.get("sm_network") if consume_value.get("sm_network") is not None else ""
            message.sm_jailBreak = consume_value.get("sm_jailBreak") if consume_value.get("sm_jailBreak") is not None else ""
            message.sm_roaming = consume_value.get("sm_roaming") if consume_value.get("sm_roaming") is not None else ""
            message.sm_wifiApSsid = consume_value.get("sm_wifiApSsid") if consume_value.get("sm_wifiApSsid") is not None else ""
            message.sm_mobileAPSsid = consume_value.get("sm_mobileAPSsid") if consume_value.get("sm_mobileAPSsid") is not None else ""

            message.workGbn = consume_value.get("workGbn") if consume_value.get("workGbn") is not None else ""
            message.workType = consume_value.get("workType") if consume_value.get("workType") is not None else ""
            message.FDS_IDEN = consume_value.get("FDS_IDEN") if consume_value.get("FDS_IDEN") is not None else ""
            message.securityMediaType = consume_value.get("securityMediaType") if consume_value.get("securityMediaType") is not None else ""
            message.country = consume_value.get("country") if consume_value.get("country") is not None else ""
            message.doaddress = consume_value.get("doaddress") if consume_value.get("doaddress") is not None else ""
            message.blockingType = consume_value.get("blockingType") if consume_value.get("blockingType") is not None else ""
            message.isForeigner = consume_value.get("isForeigner") if consume_value.get("isForeigner") is not None else ""
            message.isNewAccount = consume_value.get("isNewAccount") if consume_value.get("isNewAccount") is not None else ""
            message.isNewDevice = consume_value.get("isNewDevice") if consume_value.get("isNewDevice") is not None else ""

        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")
        return message
