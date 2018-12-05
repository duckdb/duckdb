/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Matt Emmerton
 */

/******************************************************************************
 *   Description:        This file implements the interface for data logging.
 ******************************************************************************/

#include <iostream>
#include <sstream>

#include "utilities/EGenStandardTypes.h"
#include "main/DriverTypes.h"
#include "main/DriverParamSettings.h"
#include "main/BaseLogger.h"
#include "main/BaseLogFormatter.h"

using namespace TPCE;

char TPCE::szDriverTypeNames[eDriverMax][14] = {"EGenLoader", "EGenDriverAll", "EGenDriverCE", "EGenDriverMEE",
                                                "EGenDriverDM"};

/* Constructor */
CBaseLogger::CBaseLogger(eDriverType drvType, INT32 UniqueId, CBaseLogFormatter *pFormatter)
    : m_pLogFormatter(pFormatter) {
	char m_Version[32];

	// Get EGen Version
	GetEGenVersionString(m_Version, sizeof(m_Version));

	// Generate Log Prefix String
	snprintf(&m_Prefix[0], sizeof(m_Prefix), "%s (%s) %d", szDriverTypeNames[drvType], m_Version, UniqueId);
}

/* Methods */
bool CBaseLogger::SendToLogger(const char *szPrefix, const char *szMsg) {
	CDateTime curTime;
	return SendToLoggerImpl(szPrefix, curTime.ToStr(12), szMsg);
}

// Strings
bool CBaseLogger::SendToLogger(const char *str) {
	return SendToLogger(m_Prefix, str);
}
bool CBaseLogger::SendToLogger(string str) {
	return SendToLogger(str.c_str());
}

// Parameter Structures
bool CBaseLogger::SendToLogger(CLoaderSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}
bool CBaseLogger::SendToLogger(CDriverGlobalSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CDriverCESettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CDriverCEPartitionSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CDriverMEESettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CDriverDMSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CBrokerVolumeSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CCustomerPositionSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CMarketWatchSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CSecurityDetailSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CTradeLookupSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CTradeOrderSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CTradeUpdateSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(CTxnMixGeneratorSettings &parms) {
	return SendToLogger(m_pLogFormatter->GetLogOutput(parms));
}

bool CBaseLogger::SendToLogger(TDriverCETxnSettings &parms) {
	bool result = false;

	result |= SendToLogger(m_pLogFormatter->GetLogOutput(parms.BV_settings));
	result |= SendToLogger(m_pLogFormatter->GetLogOutput(parms.CP_settings));
	result |= SendToLogger(m_pLogFormatter->GetLogOutput(parms.MW_settings));
	result |= SendToLogger(m_pLogFormatter->GetLogOutput(parms.SD_settings));
	result |= SendToLogger(m_pLogFormatter->GetLogOutput(parms.TL_settings));
	result |= SendToLogger(m_pLogFormatter->GetLogOutput(parms.TO_settings));
	result |= SendToLogger(m_pLogFormatter->GetLogOutput(parms.TU_settings));
	result |= SendToLogger(m_pLogFormatter->GetLogOutput(parms.TxnMixGenerator_settings));

	return result;
}
