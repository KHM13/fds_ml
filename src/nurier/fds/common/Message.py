from multipledispatch import dispatch


class Message:
    TR_DTM: str
    EBNK_MED_DSC: str
    COPR_DS: str
    LANG_DS: str
    NBNK_C: str
    E_FNC_USR_OS_DSC: str
    E_FNC_LGIN_DSC: str
    RMS_SVC_C: str
    E_FNC_USR_DVIC_INF_CNTN: str
    E_FNC_USR_ACS_DSC: str
    E_FNC_MED_SVCID: str
    E_FNC_MED_SVRNM: str
    E_FNC_RSP_C: str
    EXE_YN: str
    Amount: float
    E_FNC_TR_ACNO_C: str
    IO_EA_PW_CD_DS1: str
    IO_EA_PW_CD_DS2: str
    IO_EA_PW_CD_DS3: str
    PRE_ASSIGN_YN: str
    SMS_AUTHEN_YN: str
    EXCEPT_REGIST: str
    EXCEPTION_ADD_AUTHEN_YN: str
    SMART_AUTHEN_YN: str
    ATTC_DS: str
    FTR_DS2: str
    RV_AC_DGN_YN2: str
    IO_EA_RMT_FEE1: float
    IO_EA_DD1_FTR_LMT3: float
    IO_EA_TM1_FTR_LMT3: float
    IO_EA_DPZ_PL_IMP_BAC: float
    IO_EA_TOT_BAC6: float
    pc_isProxy: str
    pc_isVpn: str
    pc_os: str
    pc_OsLangCd: str
    pc_OsRemoteYn: str
    pc_OS_FIREWALL_CD: str
    pc_isVm: str
    pc_vmName: str
    pc_REMOTE_YN: str
    pc_RemoteProg: str
    pc_RemotePORT: str
    pc_remoteInfo1: str
    pc_remoteInfo2: str
    pc_remoteInfo3: str
    pc_remoteInfo4: str
    pc_remoteInfo5: str
    pc_remoteInfo6: str
    pc_remoteInfo7: str
    pc_remoteInfo8: str
    pc_foresicInfo: str
    pc_isWinDefender: str
    pc_isWinFirewall: str
    pc_isCertMisuse: str
    pc_PubIpCntryCd: str
    pc_PrxyCntryCd: str
    pc_VpnCntryCd: str
    pc_FORGERY_MAC_YN: str
    pc_FORGERY_MAC_ETH0_YN: str
    pc_FORGERY_MAC_ETH1_YN: str
    pc_FORGERY_MAC_ETH2_YN: str
    pc_FORGERY_MAC_ETH3_YN: str
    pc_FORGERY_MAC_ETH4_YN: str
    pc_FORGERY_MAC_ETH5_YN: str
    pc_HdModel: str
    pc_BwVsnCd: str
    pc_SCAN_CNT_DETECT: str
    pc_SCAN_CNT_CURED: str
    sm_deviceModel: str
    sm_osVersion: str
    sm_service: str
    sm_locale: str
    sm_network: str
    sm_jailBreak: str
    sm_roaming: str
    sm_wifiApSsid: str
    sm_mobileAPSsid: str
    workGbn: str
    workType: str
    FDS_IDEN: str
    securityMediaType: str
    country: str
    doaddress: str
    blockingType: str
    totalScore: float
    processState: str
    isForeigner: str
    isNewAccount: str
    isNewDevice: str

    @dispatch()
    def __init__(self):
        self.TR_DTM = ""
        self.EBNK_MED_DSC = ""
        self.COPR_DS = ""
        self.LANG_DS = ""
        self.NBNK_C = ""
        self.E_FNC_USR_OS_DSC = ""
        self.E_FNC_LGIN_DSC = ""
        self.RMS_SVC_C = ""
        self.E_FNC_USR_DVIC_INF_CNTN = ""
        self.E_FNC_USR_ACS_DSC = ""
        self.E_FNC_MED_SVCID = ""
        self.E_FNC_MED_SVRNM = ""
        self.E_FNC_RSP_C = ""
        self.EXE_YN = ""
        self.Amount = 0.0
        self.E_FNC_TR_ACNO_C = ""
        self.IO_EA_PW_CD_DS1 = ""
        self.IO_EA_PW_CD_DS2 = ""
        self.IO_EA_PW_CD_DS3 = ""
        self.PRE_ASSIGN_YN = ""
        self.SMS_AUTHEN_YN = ""
        self.EXCEPT_REGIST = ""
        self.EXCEPTION_ADD_AUTHEN_YN = ""
        self.SMART_AUTHEN_YN = ""
        self.ATTC_DS = ""
        self.FTR_DS2 = ""
        self.RV_AC_DGN_YN2 = ""
        self.IO_EA_RMT_FEE1 = 0.0
        self.IO_EA_DD1_FTR_LMT3 = 0.0
        self.IO_EA_TM1_FTR_LMT3 = 0.0
        self.IO_EA_DPZ_PL_IMP_BAC = 0.0
        self.IO_EA_TOT_BAC6 = 0.0
        self.pc_isProxy = ""
        self.pc_isVpn = ""
        self.pc_os = ""
        self.pc_OsLangCd = ""
        self.pc_OsRemoteYn = ""
        self.pc_OS_FIREWALL_CD = ""
        self.pc_isVm = ""
        self.pc_vmName = ""
        self.pc_REMOTE_YN = ""
        self.pc_RemoteProg = ""
        self.pc_RemotePORT = ""
        self.pc_remoteInfo1 = ""
        self.pc_remoteInfo2 = ""
        self.pc_remoteInfo3 = ""
        self.pc_remoteInfo4 = ""
        self.pc_remoteInfo5 = ""
        self.pc_remoteInfo6 = ""
        self.pc_remoteInfo7 = ""
        self.pc_remoteInfo8 = ""
        self.pc_foresicInfo = ""
        self.pc_isWinDefender = ""
        self.pc_isWinFirewall = ""
        self.pc_isCertMisuse = ""
        self.pc_PubIpCntryCd = ""
        self.pc_PrxyCntryCd = ""
        self.pc_VpnCntryCd = ""
        self.pc_FORGERY_MAC_YN = ""
        self.pc_FORGERY_MAC_ETH0_YN = ""
        self.pc_FORGERY_MAC_ETH1_YN = ""
        self.pc_FORGERY_MAC_ETH2_YN = ""
        self.pc_FORGERY_MAC_ETH3_YN = ""
        self.pc_FORGERY_MAC_ETH4_YN = ""
        self.pc_FORGERY_MAC_ETH5_YN = ""
        self.pc_HdModel = ""
        self.pc_BwVsnCd = ""
        self.pc_SCAN_CNT_DETECT = ""
        self.pc_SCAN_CNT_CURED = ""
        self.sm_deviceModel = ""
        self.sm_osVersion = ""
        self.sm_service = ""
        self.sm_locale = ""
        self.sm_network = ""
        self.sm_jailBreak = ""
        self.sm_roaming = ""
        self.sm_wifiApSsid = ""
        self.sm_mobileAPSsid = ""
        self.workGbn = ""
        self.workType = ""
        self.FDS_IDEN = ""
        self.securityMediaType = ""
        self.country = ""
        self.doaddress = ""
        self.blockingType = ""
        self.totalScore = 0.0
        self.processState = ""
        self.isForeigner = ""
        self.isNewAccount = ""
        self.isNewDevice = ""

    @dispatch(str)
    def __init__(self, message):
        message_list = message.split(",")
        print(message_list)

    def _set_message(self, TR_DTM, EBNK_MED_DSC, COPR_DS, LANG_DS, NBNK_C, E_FNC_USR_OS_DSC,
                 E_FNC_LGIN_DSC, RMS_SVC_C, E_FNC_USR_DVIC_INF_CNTN, E_FNC_USR_ACS_DSC, E_FNC_MED_SVCID,
                 E_FNC_MED_SVRNM, E_FNC_RSP_C, EXE_YN, Amount, E_FNC_TR_ACNO_C, IO_EA_PW_CD_DS1, IO_EA_PW_CD_DS2, IO_EA_PW_CD_DS3,
                 PRE_ASSIGN_YN, SMS_AUTHEN_YN, EXCEPT_REGIST, EXCEPTION_ADD_AUTHEN_YN, SMART_AUTHEN_YN, ATTC_DS,
                 FTR_DS2, RV_AC_DGN_YN2, IO_EA_RMT_FEE1, IO_EA_DD1_FTR_LMT3, IO_EA_TM1_FTR_LMT3, IO_EA_DPZ_PL_IMP_BAC, IO_EA_TOT_BAC6,
                 pc_isProxy, pc_isVpn, pc_os, pc_OsLangCd, pc_OsRemoteYn, pc_OS_FIREWALL_CD, pc_isVm, pc_vmName,
                 pc_REMOTE_YN, pc_RemoteProg, pc_RemotePORT, pc_remoteInfo1, pc_remoteInfo2, pc_remoteInfo3,
                 pc_remoteInfo4, pc_remoteInfo5, pc_remoteInfo6, pc_remoteInfo7, pc_remoteInfo8, pc_foresicInfo, pc_isWinDefender,
                 pc_isWinFirewall, pc_isCertMisuse, pc_PubIpCntryCd, pc_PrxyCntryCd, pc_VpnCntryCd, pc_FORGERY_MAC_YN,
                 pc_FORGERY_MAC_ETH0_YN, pc_FORGERY_MAC_ETH1_YN, pc_FORGERY_MAC_ETH2_YN, pc_FORGERY_MAC_ETH3_YN, pc_FORGERY_MAC_ETH4_YN,
                 pc_FORGERY_MAC_ETH5_YN, pc_HdModel, pc_BwVsnCd, pc_SCAN_CNT_DETECT, pc_SCAN_CNT_CURED,
                 sm_deviceModel, sm_osVersion, sm_service, sm_locale, sm_network, sm_jailBreak, sm_roaming, sm_wifiApSsid,
                 sm_mobileAPSsid, workGbn, workType, FDS_IDEN, securityMediaType, country, doaddress,
                 blockingType, totalScore, processState, isForeigner, isNewAccount, isNewDevice):
        self.TR_DTM = TR_DTM
        self.EBNK_MED_DSC = EBNK_MED_DSC
        self.COPR_DS = COPR_DS
        self.LANG_DS = LANG_DS
        self.NBNK_C = NBNK_C
        self.E_FNC_USR_OS_DSC = E_FNC_USR_OS_DSC
        self.E_FNC_LGIN_DSC = E_FNC_LGIN_DSC
        self.RMS_SVC_C = RMS_SVC_C
        self.E_FNC_USR_DVIC_INF_CNTN = E_FNC_USR_DVIC_INF_CNTN
        self.E_FNC_USR_ACS_DSC = E_FNC_USR_ACS_DSC
        self.E_FNC_MED_SVCID = E_FNC_MED_SVCID
        self.E_FNC_MED_SVRNM = E_FNC_MED_SVRNM
        self.E_FNC_RSP_C = E_FNC_RSP_C
        self.EXE_YN = EXE_YN
        self.Amount = Amount
        self.E_FNC_TR_ACNO_C = E_FNC_TR_ACNO_C
        self.IO_EA_PW_CD_DS1 = IO_EA_PW_CD_DS1
        self.IO_EA_PW_CD_DS2 = IO_EA_PW_CD_DS2
        self.IO_EA_PW_CD_DS3 = IO_EA_PW_CD_DS3
        self.PRE_ASSIGN_YN = PRE_ASSIGN_YN
        self.SMS_AUTHEN_YN = SMS_AUTHEN_YN
        self.EXCEPT_REGIST = EXCEPT_REGIST
        self.EXCEPTION_ADD_AUTHEN_YN = EXCEPTION_ADD_AUTHEN_YN
        self.SMART_AUTHEN_YN = SMART_AUTHEN_YN
        self.ATTC_DS = ATTC_DS
        self.FTR_DS2 = FTR_DS2
        self.RV_AC_DGN_YN2 = RV_AC_DGN_YN2
        self.IO_EA_RMT_FEE1 = IO_EA_RMT_FEE1
        self.IO_EA_DD1_FTR_LMT3 = IO_EA_DD1_FTR_LMT3
        self.IO_EA_TM1_FTR_LMT3 = IO_EA_TM1_FTR_LMT3
        self.IO_EA_DPZ_PL_IMP_BAC = IO_EA_DPZ_PL_IMP_BAC
        self.IO_EA_TOT_BAC6 = IO_EA_TOT_BAC6
        self.pc_isProxy = pc_isProxy
        self.pc_isVpn = pc_isVpn
        self.pc_os = pc_os
        self.pc_OsLangCd = pc_OsLangCd
        self.pc_OsRemoteYn = pc_OsRemoteYn
        self.pc_OS_FIREWALL_CD = pc_OS_FIREWALL_CD
        self.pc_isVm = pc_isVm
        self.pc_vmName = pc_vmName
        self.pc_REMOTE_YN = pc_REMOTE_YN
        self.pc_RemoteProg = pc_RemoteProg
        self.pc_RemotePORT = pc_RemotePORT
        self.pc_remoteInfo1 = pc_remoteInfo1
        self.pc_remoteInfo2 = pc_remoteInfo2
        self.pc_remoteInfo3 = pc_remoteInfo3
        self.pc_remoteInfo4 = pc_remoteInfo4
        self.pc_remoteInfo5 = pc_remoteInfo5
        self.pc_remoteInfo6 = pc_remoteInfo6
        self.pc_remoteInfo7 = pc_remoteInfo7
        self.pc_remoteInfo8 = pc_remoteInfo8
        self.pc_foresicInfo = pc_foresicInfo
        self.pc_isWinDefender = pc_isWinDefender
        self.pc_isWinFirewall = pc_isWinFirewall
        self.pc_isCertMisuse = pc_isCertMisuse
        self.pc_PubIpCntryCd = pc_PubIpCntryCd
        self.pc_PrxyCntryCd = pc_PrxyCntryCd
        self.pc_VpnCntryCd = pc_VpnCntryCd
        self.pc_FORGERY_MAC_YN = pc_FORGERY_MAC_YN
        self.pc_FORGERY_MAC_ETH0_YN = pc_FORGERY_MAC_ETH0_YN
        self.pc_FORGERY_MAC_ETH1_YN = pc_FORGERY_MAC_ETH1_YN
        self.pc_FORGERY_MAC_ETH2_YN = pc_FORGERY_MAC_ETH2_YN
        self.pc_FORGERY_MAC_ETH3_YN = pc_FORGERY_MAC_ETH3_YN
        self.pc_FORGERY_MAC_ETH4_YN = pc_FORGERY_MAC_ETH4_YN
        self.pc_FORGERY_MAC_ETH5_YN = pc_FORGERY_MAC_ETH5_YN
        self.pc_HdModel = pc_HdModel
        self.pc_BwVsnCd = pc_BwVsnCd
        self.pc_SCAN_CNT_DETECT = pc_SCAN_CNT_DETECT
        self.pc_SCAN_CNT_CURED = pc_SCAN_CNT_CURED
        self.sm_deviceModel = sm_deviceModel
        self.sm_osVersion = sm_osVersion
        self.sm_service = sm_service
        self.sm_locale = sm_locale
        self.sm_network = sm_network
        self.sm_jailBreak = sm_jailBreak
        self.sm_roaming = sm_roaming
        self.sm_wifiApSsid = sm_wifiApSsid
        self.sm_mobileAPSsid = sm_mobileAPSsid
        self.workGbn = workGbn
        self.workType = workType
        self.FDS_IDEN = FDS_IDEN
        self.securityMediaType = securityMediaType
        self.country = country
        self.doaddress = doaddress
        self.blockingType = blockingType
        self.totalScore = totalScore
        self.processState = processState
        self.isForeigner = isForeigner
        self.isNewAccount = isNewAccount
        self.isNewDevice = isNewDevice

    def _to_string(self):
        return f"Message [TR_DTM={self.TR_DTM}, EBNK_MED_DSC={self.EBNK_MED_DSC}, COPR_DS={self.COPR_DS}, " \
               f"LANG_DS={self.LANG_DS}, NBNK_C={self.NBNK_C}, E_FNC_USR_OS_DSC={self.E_FNC_USR_OS_DSC}, " \
               f"E_FNC_LGIN_DSC={self.E_FNC_LGIN_DSC}, RMS_SVC_C={self.RMS_SVC_C}, E_FNC_USR_DVIC_INF_CNTN= {self.E_FNC_USR_DVIC_INF_CNTN}, " \
               f"E_FNC_USR_ACS_DSC={self.E_FNC_USR_ACS_DSC}, E_FNC_MED_SVCID={self.E_FNC_MED_SVCID}, E_FNC_MED_SVRNM={self.E_FNC_MED_SVRNM}, " \
               f"E_FNC_RSP_C={self.E_FNC_RSP_C}, EXE_YN={self.EXE_YN}, Amount={self.Amount}, E_FNC_TR_ACNO_C={self.E_FNC_TR_ACNO_C}, " \
               f"IO_EA_PW_CD_DS1={self.IO_EA_PW_CD_DS1}, IO_EA_PW_CD_DS2={self.IO_EA_PW_CD_DS2}, IO_EA_PW_CD_DS3={self.IO_EA_PW_CD_DS3}, " \
               f"PRE_ASSIGN_YN={self.PRE_ASSIGN_YN}, SMS_AUTHEN_YN={self.SMS_AUTHEN_YN}, EXCEPT_REGIST={self.EXCEPT_REGIST}, " \
               f"EXCEPTION_ADD_AUTHEN_YN={self.EXCEPTION_ADD_AUTHEN_YN}, SMART_AUTHEN_YN={self.SMART_AUTHEN_YN}, ATTC_DS={self.ATTC_DS}, " \
               f"RV_AC_DGN_YN2={self.RV_AC_DGN_YN2}, IO_EA_RMT_FEE1={self.IO_EA_RMT_FEE1}, IO_EA_DD1_FTR_LMT3={self.IO_EA_DD1_FTR_LMT3}, " \
               f"IO_EA_TM1_FTR_LMT3={self.IO_EA_TM1_FTR_LMT3}, IO_EA_DPZ_PL_IMP_BAC={self.IO_EA_DPZ_PL_IMP_BAC}, " \
               f"IO_EA_TOT_BAC6={self.IO_EA_TOT_BAC6}, pc_isProxy={self.pc_isProxy}, pc_isVpn={self.pc_isVpn}, " \
               f"pc_os={self.pc_os}, pc_OsLangCd={self.pc_OsLangCd}, pc_OsRemoteYn={self.pc_OsRemoteYn}, " \
               f"pc_OS_FIREWALL_CD={self.pc_OS_FIREWALL_CD}, pc_isVm={self.pc_isVm}, pc_vmName={self.pc_vmName}, " \
               f"pc_REMOTE_YN={self.pc_REMOTE_YN}, pc_RemoteProg={self.pc_RemoteProg}, pc_RemotePORT={self.pc_RemotePORT}, " \
               f"pc_remoteInfo1={self.pc_remoteInfo1}, pc_remoteInfo2={self.pc_remoteInfo2}, pc_remoteInfo3={self.pc_remoteInfo3}, " \
               f"pc_remoteInfo4={self.pc_remoteInfo4}, pc_remoteInfo5={self.pc_remoteInfo5}, pc_remoteInfo6={self.pc_remoteInfo6}, " \
               f"pc_remoteInfo7={self.pc_remoteInfo7}, pc_remoteInfo8={self.pc_remoteInfo8}, pc_foresicInfo={self.pc_foresicInfo}, " \
               f"pc_isWinDefender={self.pc_isWinDefender}, pc_isWinFirewall={self.pc_isWinFirewall}, pc_isCertMisuse={self.pc_isCertMisuse}, " \
               f"pc_PubIpCntryCd={self.pc_PubIpCntryCd}, pc_PrxyCntryCd={self.pc_PrxyCntryCd}, pc_VpnCntryCd={self.pc_VpnCntryCd}, " \
               f"pc_SCAN_CNT_CURED={self.pc_SCAN_CNT_CURED}, pc_SCAN_CNT_DETECT={self.pc_SCAN_CNT_DETECT}, " \
               f"pc_HdModel={self.pc_HdModel}, pc_BwVsnCd={self.pc_BwVsnCd}, pc_FORGERY_MAC_YN={self.pc_FORGERY_MAC_YN}, " \
               f"pc_FORGERY_MAC_ETH0_YN={self.pc_FORGERY_MAC_ETH0_YN}, pc_FORGERY_MAC_ETH1_YN={self.pc_FORGERY_MAC_ETH1_YN}, " \
               f"pc_FORGERY_MAC_ETH2_YN={self.pc_FORGERY_MAC_ETH2_YN}, pc_FORGERY_MAC_ETH3_YN={self.pc_FORGERY_MAC_ETH3_YN}, " \
               f"pc_FORGERY_MAC_ETH4_YN={self.pc_FORGERY_MAC_ETH4_YN}, pc_FORGERY_MAC_ETH5_YN={self.pc_FORGERY_MAC_ETH5_YN}, " \
               f"sm_deviceModel={self.sm_deviceModel}, sm_osVersion={self.sm_osVersion}, sm_service={self.sm_service}, " \
               f"sm_locale={self.sm_locale}, sm_network={self.sm_network}, sm_jailBreak={self.sm_jailBreak}, sm_roaming={self.sm_roaming}, " \
               f"sm_wifiApSsid={self.sm_wifiApSsid}, sm_mobileAPSsid={self.sm_mobileAPSsid}, workGbn={self.workGbn}, " \
               f"workType={self.workType}, FDS_IDEN={self.FDS_IDEN}, securityMediaType={self.securityMediaType}, " \
               f"country={self.country}, doaddress={self.doaddress}, blockingType={self.blockingType}, totalScore={self.totalScore}, " \
               f"processState={self.processState}, isForeigner={self.isForeigner}, isNewAccount={self.isNewAccount}, isNewDevice={self.isNewDevice}"
