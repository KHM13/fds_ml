from src.nurier.ml.common.CommonProperties import CommonProperties as prop
from src.nurier.ml.common.CommonUtil import CommonUtil as util
from src.nurier.fds.common.LoggingHandler import LoggingHandler
from src.nurier.fds.common.StaticConstant import StaticConstant as const
from src.nurier.fds.common.EnumData import MessageLog as log
from src.nurier.fds.common.Message import Message
from datetime import datetime
from multipledispatch import dispatch
import time

logger = LoggingHandler(f"{prop().get_result_log_file_path()}{util().now_type3()}_output", "a", "DEBUG")
logger = logger.get_log()


class StaticCommonUtil:
    __FASTDATE_TYPE_1 = "%Y-%m-%d %H:%M:%S"
    __FASTDATE_TYPE_2 = "%Y.%m.%d"
    __FASTDATE_TYPE_3 = "%Y%m%d%H%M%S"
    __FASTDATE_TYPE_4 = "%Y%m%d"
    __FASTDATE_TYPE_5 = "%Y"
    __FASTDATE_TYPE_6 = "%H"
    __FASTDATE_TYPE_7 = "%M"
    __FASTDATE_TYPE_8 = "%S"

    FASTDATE_TYPE_yyyy_MM = "%Y.%m"
    INQUIRY_RMS_SVC_C = ["EANBMM98R0", "EANBMM16R0"]

    def getFASTDATE_TYPE_1(self) -> str:
        return self.__FASTDATE_TYPE_1

    def getFASTDATE_TYPE_2(self) -> str:
        return self.__FASTDATE_TYPE_2

    def getFASTDATE_TYPE_3(self) -> str:
        return self.__FASTDATE_TYPE_3

    def getFASTDATE_TYPE_4(self) -> str:
        return self.__FASTDATE_TYPE_4

    def getFASTDATE_TYPE_5(self) -> str:
        return self.__FASTDATE_TYPE_5

    def getFASTDATE_TYPE_6(self) -> str:
        return self.__FASTDATE_TYPE_6

    def getFASTDATE_TYPE_7(self) -> str:
        return self.__FASTDATE_TYPE_7

    def getFASTDATE_TYPE_8(self) -> str:
        return self.__FASTDATE_TYPE_8

    @staticmethod
    def getDateFormatToDate(date_format_type: str, source: str) -> datetime:
        try:
            result = datetime.strptime(source, date_format_type)
        except Exception as e:
            result = datetime
            print(f"[StaticCommonUtil][getDateFormatToDate] ERROR : {e}")
            logger.error(f"[StaticCommonUtil][getDateFormatToDate] ERROR : {e}")
        return result

    @staticmethod
    def getDateFormatToString(date_format_type: str, date: datetime) -> str:
        try:
            return date.strftime(date_format_type)
        except:
            return datetime.strftime(date_format_type)

    def getDateFormatToStringBySTD_GBL_ID(self, date_format_type: str, std_gbl_id: str) -> str:
        try:
            if not self.isNullString(std_gbl_id):
                #tr_dtm = buffer("20")
                tr_dtm = ''.join(["20", std_gbl_id[:12]])

                if len(tr_dtm) == 14:
                    return self.getDateFormatToString(date_format_type, self.getDateFormatToDate(self.__FASTDATE_TYPE_3, tr_dtm))
                else:
                    return self.getDateFormatToString(date_format_type, datetime)
            else:
                return self.getDateFormatToString(date_format_type, datetime)
        except Exception as e:
            print("[StaticCommonUtil.getDateFormatToString][Exception]")
            logger.error(e)

            try:
                return self.getDateFormatToString(date_format_type, datetime)
            except Exception as e:
                print("[StaticCommonUtil.getDateFormatToString][Exception]")
                logger.error(e)
        return ""

    def getTr_dtmFromStd_gbl_id(self, std_gbl_id: str) -> str:
        try:
            if not self.isNullString(std_gbl_id):
                tr_dtm = ''.join(["20", std_gbl_id[:12]])

                if len(tr_dtm) == 14:
                    return self.getDateFormatToString(self.__FASTDATE_TYPE_1, self.getDateFormatToDate(self.__FASTDATE_TYPE_3, tr_dtm))
                else:
                    return self.getDateFormatToString(self.__FASTDATE_TYPE_1, datetime)
            else:
                return self.getDateFormatToString(self.__FASTDATE_TYPE_1, datetime)
        except Exception as e:
            print("\t\t\t[StaticCommonUtil.getTr_dtmFromStd_gbl_id_1][Exception]")
            print(f"{self.__class__} ERROR : {e}")
            logger.error(f"{self.__class__} ERROR : {e}")

            try:
                return self.getDateFormatToString(self.__FASTDATE_TYPE_1, datetime)
            except Exception as e:
                print("\t\t\t[StaticCommonUtil.getTr_dtmFromStd_gbl_id_2][Exception]")
                print(f"{self.__class__} ERROR : {e}")
                logger.error(f"{self.__class__} ERROR : {e}")
        return ""

    def isInquiryEqualsValue(self, value: str) -> bool:
        result = False
        for s in self.INQUIRY_RMS_SVC_C:
            if value.__eq__(s):
                result = True
                break
        return result

    @staticmethod
    def getNanoTime():
        return time.time_ns()

    @staticmethod
    def isNullString(value: str) -> bool:
        result = False
        if value is None or value.strip().__eq__("") or value.strip().__eq__("null"):
            result = True
        return result

    @staticmethod
    def isNumberic(value: str) -> bool:
        result = False
        if value is not None:
            result = value.isnumeric()
        return result

    @staticmethod
    def toString_fromInt(value: int) -> str:
        return str(value)

    @staticmethod
    def toString_fromFloat(value: float) -> str:
        return str(value)

    def toInt_fromString(self, value: str) -> int:
        try:
            if self.isNullString(value):
                return 0
            return int(value)
        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")
            return 0

    @dispatch(float)
    def toInt(self, value: float) -> int:
        if value is not None:
            return int(value)
        return 0

    @dispatch(str)
    def toInt(self, value: str) -> int:
        return int(self.toFloat_fromString(value))

    def toFloat_fromString(self, value: str) -> float:
        if self.isNullString(value):
            value = "0"
        elif not self.isNumberic(value):
            value = "0"
        return float(value)

    @staticmethod
    def get_split(value: str, seperator):
        if value is not None and seperator is not None:
            return value.split(seperator)
        else:
            return None

    @staticmethod
    def get_subString(value: str, start: int, end: int) -> str:
        if value is not None and start < end & len(value) >= end:
            return value[start:end]
        else:
            return ""

    @staticmethod
    def get_between_blackresult(value1: str, value2: str) -> str:
        try:
            v1: int = const.BLACKRESULT_MAP.get(value1)
            v2: int = const.BLACKRESULT_MAP.get(value2)

            if v1 > v2:
                return value1
            elif v1 < v2:
                return value2
            else:
                return value1
        except Exception as e:
            print(f"StaticCommonUtil ERROR : {e}")
            logger.info("[Error] : get_between_blackresult ({}, {})".format(value1, value2))
        return value1

    @staticmethod
    def get_between_fdsresult(value1: str, value2: str) -> str:
        v1: int = const.FDSRESULT_MAP.get(value1)
        v2: int = const.FDSRESULT_MAP.get(value2)

        if v1 > v2:
            return value1
        elif v1 < v2:
            return value2
        else:
            value1

    def get_blacklistfield(self, message: Message):
        commonField = []
        if not self.isNullString(message.E_FNC_USRID):
            commonField.append(message.E_FNC_USRID)
        if not self.isNullString(message.pc_publicIP1):
            commonField.append(message.pc_publicIP1)
        if not self.isNullString(message.pc_publicIP2):
            commonField.append(message.pc_publicIP2)
        if not self.isNullString(message.pc_publicIP3):
            commonField.append(message.pc_publicIP3)
        if not self.isNullString(message.pc_macAddr1):
            commonField.append(message.pc_macAddr1)
        if not self.isNullString(message.pc_macAddr2):
            commonField.append(message.pc_macAddr2)
        if not self.isNullString(message.pc_macAddr3):
            commonField.append(message.pc_macAddr3)
        if not self.isNullString(message.pc_hddSerial1):
            commonField.append(message.pc_hddSerial1)
        if not self.isNullString(message.pc_hddSerial2):
            commonField.append(message.pc_hddSerial2)
        if not self.isNullString(message.pc_hddSerial3):
            commonField.append(message.pc_hddSerial3)
        return commonField

    def get_remotelistfield(self, message: Message):
        commonField = []
        if not self.isNullString(message.pc_remoteInfo1) & len(message.pc_remoteInfo1) >= 10:
            commonField.append(message.pc_remoteInfo1)
        if not self.isNullString(message.pc_remoteInfo2) & len(message.pc_remoteInfo2) >= 10:
            commonField.append(message.pc_remoteInfo2)
        if not self.isNullString(message.pc_remoteInfo3) & len(message.pc_remoteInfo3) >= 10:
            commonField.append(message.pc_remoteInfo3)
        if not self.isNullString(message.pc_remoteInfo4) & len(message.pc_remoteInfo4) >= 10:
            commonField.append(message.pc_remoteInfo4)
        if not self.isNullString(message.pc_remoteInfo5) & len(message.pc_remoteInfo5) >= 10:
            commonField.append(message.pc_remoteInfo5)
        if not self.isNullString(message.pc_remoteInfo6) & len(message.pc_remoteInfo6) >= 10:
            commonField.append(message.pc_remoteInfo6)
        if not self.isNullString(message.pc_remoteInfo7) & len(message.pc_remoteInfo7) >= 10:
            commonField.append(message.pc_remoteInfo7)
        if not self.isNullString(message.pc_remoteInfo8) & len(message.pc_remoteInfo8) >= 10:
            commonField.append(message.pc_remoteInfo8)
        return commonField

    def checkNull_pc_remoteInfo(self, message: Message):
        cnt: int = 0

        if not self.isNullString(message.pc_remoteInfo1):
            cnt += 1
        if not self.isNullString(message.pc_remoteInfo2):
            cnt += 1
        if not self.isNullString(message.pc_remoteInfo3):
            cnt += 1
        if not self.isNullString(message.pc_remoteInfo4):
            cnt += 1
        if not self.isNullString(message.pc_remoteInfo5):
            cnt += 1
        if not self.isNullString(message.pc_remoteInfo6):
            cnt += 1
        if not self.isNullString(message.pc_remoteInfo7):
            cnt += 1
        if not self.isNullString(message.pc_remoteInfo8):
            cnt += 1
        return cnt

    @staticmethod
    def getArrayList_fromStrings(values: list) -> list:
        array = []
        if values and len(values) != 0:
            for s in values:
                array.append(s)
        return array

    @dispatch(list, int, str, int)
    def addList(self, valueList: list, index: int, value: str, maxLength: int):
        result = valueList.copy()
        try:
            if not self.isNullString(value):
                result.remove(value)

                if index >= 0:
                    result.insert(index, value)
                else:
                    result.append(value)

                if maxLength != 0 & result.__sizeof__() > maxLength:
                    result.remove(result.__sizeof__() - 1)
        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")
            return valueList
        return result

    @dispatch(list, int, str, int, int)
    def addList(self, valueList: list, index: int, value: str, type1_maxSize: int, type2_maxSize: int):
        return self.addList(valueList, index, value, type1_maxSize, const.TEL_ACCOUNT_CHAR, type2_maxSize)

    @staticmethod
    @dispatch(list, int, str, int, str, int)
    def addList(valueList: list, index: int, value: str, type1_maxSize: int, type2_char: str, type2_maxSize: int):
        type1_size = 0
        type1_last_index = -1
        type2_size = 0
        type2_last_index = -1

        result = []

        loop_index = 0

        for data in valueList:
            if data.startswith(type2_char):
                type2_size += 1
                type2_last_index = loop_index
            else:
                type1_size += 1
                type1_last_index = loop_index
            result.append(data)
            loop_index += 1

            if loop_index > (type1_maxSize + type2_maxSize):
                break

        if value.startswith(type2_char) & type2_size >= type2_maxSize:
            result.remove(type2_last_index)
        elif type1_size >= type1_maxSize:
            result.remove(type1_last_index)
        result.remove(value)

        if index >= 0:
            result.insert(index, value)
        else:
            result.append(value)

        return result

    @staticmethod
    def check_tel_number(value: str) -> str:
        return value

    @staticmethod
    @dispatch(dict, str)
    def is_init_hashMap(map: dict, string_date: str) -> bool:
        if map is not None:
            if map.get("update") is not None:
                if not string_date.__eq__(map.get("update")):
                    return True
            else:
                return True
        else:
            return True
        return False

    @staticmethod
    @dispatch(dict, int)
    def is_init_hashMap(map: dict, int_date: int) -> bool:
        if map is not None:
            if map.get("update") is not None:
                if map.get("update") != int_date:
                    return True
            else:
                return True
        else:
            return True
        return False

    @staticmethod
    @dispatch(str, dict, str)
    def is_init_hashMap(key_update: str, map: dict, date: str) -> bool:
        if map is not None:
            if map.get(key_update) is not None:
                if not date.__eq__(map.get(key_update)):
                    return True
            else:
                return True
        else:
            return True
        return False

    @staticmethod
    @dispatch(str, dict, int)
    def is_init_hashMap(key_update: str, map: dict, date: int) -> bool:
        if map is not None:
            if map.get(key_update) is not None:
                if map.get(key_update) != date:
                    return True
            else:
                return True
        else:
            return True
        return False

    @staticmethod
    @dispatch(str, dict, float)
    def is_init_hashMap(key_update: str, map: dict, date: float) -> bool:
        if map is not None:
            if map.get(key_update) is not None:
                if map.get(key_update) != date:
                    return True
            else:
                return True
        else:
            return True
        return False

    @dispatch(str, int)
    def get_diff_date(self, date1: str, date2: int) -> int:
        if date2 is None:
            date2 = 0
        return self.get_diff_date(date1, str(date2))

    @dispatch(str, float)
    def get_diff_date(self, date1: str, date2: float) -> int:
        if date2 is None:
            date2 = 0.0
        return self.get_diff_date(date1, str(date2))

    @dispatch(str, str)
    def get_diff_date(self, date1: str, date2: str) -> int:
        try:
            if date2 is None:
                return 0

            date_1 = time.mktime(self.getDateFormatToDate(self.getFASTDATE_TYPE_4(), date1).timetuple())
            date_2 = time.mktime(self.getDateFormatToDate(self.getFASTDATE_TYPE_4(), date2).timetuple())
            return (datetime.fromtimestamp(date_2) - datetime.fromtimestamp(date_1)).days
        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")
        return 0

    def get_message_value(self, message: Message, key: str) -> str:
        if not key:
            return ""
        try:
            if log.E_FNC_USRID.__eq__(key):
                return message.E_FNC_USRID
            elif log.pc_publicIP1.__eq__(key):
                return message.pc_publicIP1
            elif log.pc_publicIP2.__eq__(key):
                return message.pc_publicIP2
            elif log.pc_publicIP3.__eq__(key):
                return message.pc_publicIP3
            elif log.pc_macAddr1.__eq__(key):
                return message.pc_macAddr1
            elif log.pc_macAddr2.__eq__(key):
                return message.pc_macAddr2
            elif log.pc_macAddr3.__eq__(key):
                return message.pc_macAddr3
            elif log.pc_hddSerial1.__eq__(key):
                return message.pc_hddSerial1
            elif log.pc_hddSerial2.__eq__(key):
                return message.pc_hddSerial2
            elif log.pc_hddSerial3.__eq__(key):
                return message.pc_hddSerial3
            elif log.pc_cpuID.__eq__(key):
                return message.pc_cpuID
            elif log.TRANSFER_ACNO.__eq__(key):
                return message.TRANSFER_ACNO
            elif log.pc_mbSn.__eq__(key):
                return message.pc_mbSn
            elif log.pc_winVer.__eq__(key):
                return message.pc_winVer
            elif log.COMMON_PUBLIC_IP.__eq__(key):
                if message.COMMON_PUBLIC_IP is None:
                    return ""
                else:
                    return str(message.COMMON_PUBLIC_IP)
            elif log.pc_remoteInfo1.__eq__(key):
                return message.pc_remoteInfo1
            elif log.pc_remoteInfo2.__eq__(key):
                return message.pc_remoteInfo2
            elif log.pc_remoteInfo3.__eq__(key):
                return message.pc_remoteInfo3
            elif log.pc_remoteInfo4.__eq__(key):
                return message.pc_remoteInfo4
            elif log.pc_remoteInfo5.__eq__(key):
                return message.pc_remoteInfo5
            elif log.pc_remoteInfo6.__eq__(key):
                return message.pc_remoteInfo6
            elif log.pc_remoteInfo7.__eq__(key):
                return message.pc_remoteInfo7
            elif log.pc_remoteInfo8.__eq__(key):
                return message.pc_remoteInfo8
            elif log.IO_EA_DD1_FTR_LMT3.__eq__(key):
                return message.IO_EA_DD1_FTR_LMT3
            elif log.IO_EA_TM1_FTR_LMT3.__eq__(key):
                return message.IO_EA_TM1_FTR_LMT3
            elif log.IO_EA_PW_CD_DS1.__eq__(key):
                return message.IO_EA_PW_CD_DS1
            elif log.IO_EA_PW_CD_DS2.__eq__(key):
                return message.IO_EA_PW_CD_DS2
            elif log.IO_EA_PW_CD_DS3.__eq__(key):
                return message.IO_EA_PW_CD_DS3
            elif log.IO_EA_DRW_AC_NAME1.__eq__(key):
                return message.IO_EA_DRW_AC_NAME1
            elif log.IO_EA_RV_ACTNM1.__eq__(key):
                return message.IO_EA_RV_ACTNM1
            elif log.LS_FTR_TRDT.__eq__(key):
                return message.LS_FTR_TRDT
            elif log.LS_TRDT.__eq__(key):
                return message.LS_TRDT
            elif log.RV_AC_DGN_YN2.__eq__(key):
                return message.RV_AC_DGN_YN2
            elif log.IO_EA_DPZ_PL_IMP_BAC.__eq__(key):
                return message.IO_EA_DPZ_PL_IMP_BAC
            elif log.IO_EA_TOT_BAC6.__eq__(key):
                return message.IO_EA_TOT_BAC6
            elif log.IO_EA_RMT_FEE1.__eq__(key):
                return message.IO_EA_RMT_FEE1
            elif log.NAAC_DSC.__eq__(key):
                return message.NAAC_DSC
            elif log.EBNK_MED_DSC.__eq__(key):
                return message.EBNK_MED_DSC
            elif log.E_FNC_CUSNO.__eq__(key):
                return message.E_FNC_CUSNO
            elif log.E_FNC_COPR_ID.__eq__(key):
                return message.E_FNC_COPR_ID
            elif log.COPR_DS.__eq__(key):
                return message.COPR_DS
            elif log.LANG_DS.__eq__(key):
                return message.LANG_DS
            elif log.E_FNC_RSP_C.__eq__(key):
                return message.E_FNC_RSP_C
            elif log.E_FNC_USR_OS_DSC.__eq__(key):
                return message.E_FNC_USR_OS_DSC
            elif log.E_FNC_USR_ACS_DSC.__eq__(key):
                return message.E_FNC_USR_ACS_DSC
            elif log.E_FNC_MED_SVCID.__eq__(key):
                return message.E_FNC_MED_SVCID
            elif log.E_FNC_MED_SVRNM.__eq__(key):
                return message.E_FNC_MED_SVRNM
            elif log.Amount.__eq__(key):
                return str(message.Amount)
            elif log.E_FNC_TR_ACNO_C.__eq__(key):
                return message.E_FNC_TR_ACNO_C
            elif log.pc_privateIP1.__eq__(key):
                return message.pc_privateIP1
            elif log.pc_privateIP2.__eq__(key):
                return message.pc_privateIP2
            elif log.pc_privateIP3.__eq__(key):
                return message.pc_privateIP3
            elif log.pc_isProxy.__eq__(key):
                return message.pc_isProxy
            elif log.pc_proxyIP1.__eq__(key):
                return message.pc_proxyIP1
            elif log.pc_proxyIP2.__eq__(key):
                return message.pc_proxyIP2
            elif log.pc_isVpn.__eq__(key):
                return message.pc_isVpn
            elif log.pc_vpnIP1.__eq__(key):
                return message.pc_vpnIP1
            elif log.pc_vpnIP2.__eq__(key):
                return message.pc_vpnIP2
            elif log.pc_logicalMac1.__eq__(key):
                return message.pc_logicalMac1
            elif log.pc_logicalMac2.__eq__(key):
                return message.pc_logicalMac2
            elif log.pc_logicalMac3.__eq__(key):
                return message.pc_logicalMac3
            elif log.pc_isVm.__eq__(key):
                return message.pc_isVm
            elif log.pc_vmName.__eq__(key):
                return message.pc_vmName
            elif log.pc_gatewayMac.__eq__(key):
                return message.pc_gatewayMac
            elif log.pc_gatewayIP.__eq__(key):
                return message.pc_gatewayIP
            elif log.pc_volumeID.__eq__(key):
                return message.pc_volumeID
            elif log.pc_foresicInfo.__eq__(key):
                return message.pc_foresicInfo
            elif log.pc_isWinDefender.__eq__(key):
                return message.pc_isWinDefender
            elif log.pc_isWinFirewall.__eq__(key):
                return message.pc_isWinFirewall
            elif log.pc_isAutoPatch.__eq__(key):
                return message.pc_isAutoPatch
            elif log.sm_login_uuid.__eq__(key):
                return message.sm_login_uuid
            elif log.COMMON_PUBLIC_IP_WAS.__eq__(key):
                return message.COMMON_PUBLIC_IP_WAS
            elif log.sm_DI.__eq__(key):
                return message.sm_DI
            elif log.sm_D1.__eq__(key):
                return message.sm_D1
            elif log.sm_D2.__eq__(key):
                return message.sm_D2
            elif log.sm_deviceId.__eq__(key):
                return message.sm_deviceId
            elif log.sm_imei.__eq__(key):
                return message.sm_imei
            elif log.sm_imsi.__eq__(key):
                return message.sm_imsi
            elif log.sm_usim.__eq__(key):
                return message.sm_usim
            elif log.sm_uuid.__eq__(key):
                return message.sm_uuid
            elif log.sm_wifiMacAddr.__eq__(key):
                return message.sm_wifiMacAddr
            elif log.sm_ethernetMacAddr.__eq__(key):
                return message.sm_ethernetMacAddr
            elif log.sm_btMacAddr.__eq__(key):
                return message.sm_btMacAddr
            elif log.sm_deviceModel.__eq__(key):
                return message.sm_deviceModel
            elif log.sm_osVersion.__eq__(key):
                return message.sm_osVersion
            elif log.sm_service.__eq__(key):
                return message.sm_service
            elif log.sm_locale.__eq__(key):
                return message.sm_locale
            elif log.sm_network.__eq__(key):
                return message.sm_network
            elif log.sm_publicIP.__eq__(key):
                return message.sm_publicIP
            elif log.sm_wifi_ip.__eq__(key):
                return message.sm_wifi_ip
            elif log.sm_3g_ip.__eq__(key):
                return message.sm_3g_ip
            elif log.sm_jailBreak.__eq__(key):
                return message.sm_jailBreak
            elif log.sm_roaming.__eq__(key):
                return message.sm_roaming
            elif log.sm_proxyIp.__eq__(key):
                return message.sm_proxyIp
            elif log.sm_wifiApSsid.__eq__(key):
                return message.sm_wifiApSsid
            elif log.sm_mobileAPSsid.__eq__(key):
                return message.sm_mobileAPSsid
            elif log.sm_mobileNumber.__eq__(key):
                return message.sm_mobileNumber
            elif log.STD_GBL_ID.__eq__(key):
                return message.STD_GBL_ID
            elif log.FTR_DS2.__eq__(key):
                return message.FTR_DS2
            elif log.TR_DTM.__eq__(key):
                return message.TR_DTM
            elif log.EXE_YN.__eq__(key):
                return message.EXE_YN
            elif log.PRE_ASSIGN_YN.__eq__(key):
                return message.PRE_ASSIGN_YN
            elif log.SMS_AUTHEN_YN.__eq__(key):
                return message.SMS_AUTHEN_YN
            elif log.EXCEPT_REGIST.__eq__(key):
                return message.EXCEPT_REGIST
            elif log.EXCEPTION_ADD_AUTHEN_YN.__eq__(key):
                return message.EXCEPTION_ADD_AUTHEN_YN
            elif log.SMART_AUTHEN_YN.__eq__(key):
                return message.SMART_AUTHEN_YN
            elif log.ATTC_DS.__eq__(key):
                return message.ATTC_DS
            elif log.CHRR_TELNO.__eq__(key):
                return message.CHRR_TELNO
            elif log.CHRR_TELNO1.__eq__(key):
                return message.CHRR_TELNO1
            elif log.RG_TELNO.__eq__(key):
                return message.RG_TELNO
            elif log.WORK_GBN.__eq__(key):
                return message.WORK_GBN
            elif log.FDS_IDEN.__eq__(key):
                return message.FDS_IDEN
            elif log.E_FNC_AP_SVRNM.__eq__(key):
                return message.E_FNC_AP_SVRNM
            elif log.RES_MSG.__eq__(key):
                return message.RES_MSG
            elif log.workType.__eq__(key):
                return message.WORK_TYPE
            elif log.workGbn.__eq__(key):
                return message.WORK_GBN
            else:
                return ""
        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")
            return ""

    def get_message_name(self, key: str) -> str:
        try:
            if log.E_FNC_USRID.__eq__(key):
                return "E금융이용자ID"
            elif log.pc_publicIP1.__eq__(key):
                return "공인IP1"
            elif log.pc_publicIP2.__eq__(key):
                return "공인IP2"
            elif log.pc_publicIP3.__eq__(key):
                return "공인IP3"
            elif log.pc_macAddr1.__eq__(key):
                return "MAC Address1"
            elif log.pc_macAddr2.__eq__(key):
                return "MAC Address2"
            elif log.pc_macAddr3.__eq__(key):
                return "MAC Address3"
            elif log.pc_macAddr4.__eq__(key):
                return "MAC Address4"
            elif log.pc_macAddr5.__eq__(key):
                return "MAC Address5"
            elif log.pc_macAddr6.__eq__(key):
                return "MAC Address6"
            elif log.pc_hddSerial1.__eq__(key):
                return "HDD Serial1"
            elif log.pc_hddSerial2.__eq__(key):
                return "HDD Serial2"
            elif log.pc_hddSerial3.__eq__(key):
                return "HDD Serial3"
            elif log.pc_cpuID.__eq__(key):
                return "CPU ID"
            elif log.TRANSFER_ACNO.__eq__(key):
                return "입금계좌번호"
            elif log.pc_mbSn.__eq__(key):
                return "Mainboard SN"
            elif log.pc_winVer.__eq__(key):
                return "윈도우버전"
            elif log.COMMON_PUBLIC_IP.__eq__(key):
                return "공인IP"
            elif log.pc_remoteInfo1.__eq__(key):
                return "원격정보1"
            elif log.pc_remoteInfo2.__eq__(key):
                return "원격정보2"
            elif log.pc_remoteInfo3.__eq__(key):
                return "원격정보3"
            elif log.pc_remoteInfo4.__eq__(key):
                return "원격정보4"
            elif log.pc_remoteInfo5.__eq__(key):
                return "원격정보5"
            elif log.pc_remoteInfo6.__eq__(key):
                return "원격정보6"
            elif log.pc_remoteInfo7.__eq__(key):
                return "원격정보7"
            elif log.pc_remoteInfo8.__eq__(key):
                return "원격정보8"
            elif log.pc_remoteInfo9.__eq__(key):
                return "원격정보9"
            elif log.pc_remoteInfo10.__eq__(key):
                return "원격정보10"
            elif log.pc_isCertMisuse.__eq__(key):
                return "공인인증서 오용검사"
            elif log.IO_EA_DD1_FTR_LMT3.__eq__(key):
                return "1일이체한도"
            elif log.IO_EA_TM1_FTR_LMT3.__eq__(key):
                return "1회이체한도"
            elif log.IO_EA_PW_CD_DS1.__eq__(key):
                return "보안매체 오류횟수"
            elif log.IO_EA_PW_CD_DS2.__eq__(key):
                return "보안매체 종류코드"
            elif log.IO_EA_PW_CD_DS3.__eq__(key):
                return "키락이용여부"
            elif log.IO_EA_DRW_AC_NAME1.__eq__(key):
                return "출금계좌이름"
            elif log.IO_EA_RV_ACTNM1.__eq__(key):
                return "입금계좌이름"
            elif log.LS_FTR_TRDT.__eq__(key):
                return "최종이체거래일"
            elif log.LS_TRDT.__eq__(key):
                return "최종 로그인일자"
            elif log.RV_AC_DGN_YN2.__eq__(key):
                return "입금계좌지정여부"
            elif log.IO_EA_DPZ_PL_IMP_BAC.__eq__(key):
                return "수신지불가능잔액"
            elif log.IO_EA_TOT_BAC6.__eq__(key):
                return "출금후 잔액"
            elif log.IO_EA_RMT_FEE1.__eq__(key):
                return "송금수수료"
            elif log.NAAC_DSC.__eq__(key):
                return "중앙회조합구분코드"
            elif log.EBNK_MED_DSC.__eq__(key):
                return "E뱅킹매체구분코드"
            elif log.E_FNC_CUSNO.__eq__(key):
                return "E금융고객번호"
            elif log.E_FNC_COPR_ID.__eq__(key):
                return "E금융기업ID"
            elif log.COPR_DS.__eq__(key):
                return "기업구분"
            elif log.LANG_DS.__eq__(key):
                return "언어구분"
            elif log.E_FNC_RSP_C.__eq__(key):
                return "E금융응답코드"
            elif log.E_FNC_USR_OS_DSC.__eq__(key):
                return "E금융사용자운영체제구분코드"
            elif log.E_FNC_USR_ACS_DSC.__eq__(key):
                return "E금융사용자접근구분코드"
            elif log.E_FNC_MED_SVCID.__eq__(key):
                return "E금융매체서비스ID"
            elif log.E_FNC_MED_SVRNM.__eq__(key):
                return "E금융매체서버명"
            elif log.Amount.__eq__(key):
                return "금액"
            elif log.E_FNC_TR_ACNO_C.__eq__(key):
                return "타계좌구분코드"
            elif log.E_FNC_TR_ACNO.__eq__(key):
                return "E금융거래계좌번호"
            elif log.E_FNC_USR_IPADR.__eq__(key):
                return "E금융사용자IP주소"
            elif log.E_FNC_LGIN_DSC.__eq__(key):
                return "E금융로그인구분코드"
            elif log.E_FNC_USR_TELNO.__eq__(key):
                return "E금융사용자전화번호"
            elif log.pc_privateIP1.__eq__(key):
                return "사설IP1"
            elif log.pc_privateIP2.__eq__(key):
                return "사설IP2"
            elif log.pc_privateIP3.__eq__(key):
                return "사설IP3"
            elif log.pc_isProxy.__eq__(key):
                return "Proxy유무"
            elif log.pc_proxyIP1.__eq__(key):
                return "Proxy IP1"
            elif log.pc_proxyIP2.__eq__(key):
                return "Proxy IP2"
            elif log.pc_isVpn.__eq__(key):
                return "VPN유무"
            elif log.pc_vpnIP1.__eq__(key):
                return "VPN IP1"
            elif log.pc_vpnIP2.__eq__(key):
                return "VPN IP2"
            elif log.pc_logicalMac1.__eq__(key):
                return "논리 MAC1"
            elif log.pc_logicalMac2.__eq__(key):
                return "논리 MAC2"
            elif log.pc_logicalMac3.__eq__(key):
                return "논리 MAC3"
            elif log.pc_isVm.__eq__(key):
                return "가상OS사용여부"
            elif log.pc_vmName.__eq__(key):
                return "가상OS명"
            elif log.pc_gatewayMac.__eq__(key):
                return "게이트웨이MAC"
            elif log.pc_gatewayIP.__eq__(key):
                return "게이트웨이IP"
            elif log.pc_volumeID.__eq__(key):
                return "Disk VolumeID"
            elif log.pc_foresicInfo.__eq__(key):
                return "디지털포렌식정보"
            elif log.pc_isWinDefender.__eq__(key):
                return "Windows Defender 보안적용 여부"
            elif log.pc_isWinFirewall.__eq__(key):
                return "윈도우 방화벽 가동여부"
            elif log.pc_isAutoPatch.__eq__(key):
                return "보안패치 업데이트 자동화 여부"
            elif log.sm_login_uuid.__eq__(key):
                return "WAS UUID"
            elif log.COMMON_PUBLIC_IP_WAS.__eq__(key):
                return "WAS 공인IP"
            elif log.sm_DI.__eq__(key):
                return "기기ID 파트1"
            elif log.sm_D1.__eq__(key):
                return "기기ID 파트2"
            elif log.sm_D2.__eq__(key):
                return "기기ID 파트3"
            elif log.sm_deviceId.__eq__(key):
                return "기기일련번호"
            elif log.sm_imei.__eq__(key):
                return "IMEI"
            elif log.sm_imsi.__eq__(key):
                return "IMSI"
            elif log.sm_usim.__eq__(key):
                return "USIM 일련번호"
            elif log.sm_uuid.__eq__(key):
                return "UUID"
            elif log.sm_wifiMacAddr.__eq__(key):
                return "Wi-Fi MAC"
            elif log.sm_ethernetMacAddr.__eq__(key):
                return "이더넷 MAC"
            elif log.sm_btMacAddr.__eq__(key):
                return "블루투스 MAC"
            elif log.sm_deviceModel.__eq__(key):
                return "기기모델명"
            elif log.sm_osVersion.__eq__(key):
                return "운영체제버전"
            elif log.sm_service.__eq__(key):
                return "통신사업자"
            elif log.sm_locale.__eq__(key):
                return "국가언어코드"
            elif log.sm_network.__eq__(key):
                return "네트워크상태"
            elif log.sm_publicIP.__eq__(key):
                return "공인IP"
            elif log.sm_wifi_ip.__eq__(key):
                return "Wi-Fi 내부IP"
            elif log.sm_3g_ip.__eq__(key):
                return "3G 내부IP"
            elif log.sm_jailBreak.__eq__(key):
                return "루팅, 탈옥여부"
            elif log.sm_roaming.__eq__(key):
                return "로밍여부"
            elif log.sm_proxyIp.__eq__(key):
                return "프록시IP"
            elif log.sm_wifiApSsid.__eq__(key):
                return "WiFi AP SID"
            elif log.sm_mobileAPSsid.__eq__(key):
                return "통신사"
            elif log.sm_mobileNumber.__eq__(key):
                return "전화번호"
            elif log.STD_GBL_ID.__eq__(key):
                return "표준글로벌ID"
            elif log.FTR_DS2.__eq__(key):
                return "이체구분"
            elif log.TR_DTM.__eq__(key):
                return "거래일시"
            elif log.EXE_YN.__eq__(key):
                return "실행여부"
            elif log.PRE_ASSIGN_YN.__eq__(key):
                return "단말기지정서비스 가입여부"
            elif log.SMS_AUTHEN_YN.__eq__(key):
                return "휴대폰SMS인증서비스 가입여부"
            elif log.EXCEPT_REGIST.__eq__(key):
                return "예외고객등록여부"
            elif log.EXCEPTION_ADD_AUTHEN_YN.__eq__(key):
                return "SMS통지서비스가입여부"
            elif log.SMART_AUTHEN_YN.__eq__(key):
                return "앱인증서비스"
            elif log.ATTC_DS.__eq__(key):
                return "전화승인서비스"
            elif log.CHRR_TELNO.__eq__(key):
                return "전화승인 전화번호 1"
            elif log.CHRR_TELNO1.__eq__(key):
                return "전화승인 전화번호 2"
            elif log.RG_TELNO.__eq__(key):
                return "전화승인 전화번호 3"
            elif log.WORK_GBN.__eq__(key):
                return ""
            elif log.FDS_IDEN.__eq__(key):
                return ""
            elif log.E_FNC_AP_SVRNM.__eq__(key):
                return ""
            elif log.RES_MSG.__eq__(key):
                return ""
            elif log.E_FNC_TR_INNO.__eq__(key):
                return ""
            elif log.workType.__eq__(key):
                return "구분유형"
            elif log.workGbn.__eq__(key):
                return "구분값"
            else:
                return "No Name"
        except Exception as e:
            print(f"{self.__class__} ERROR : {e}")
            return ""
