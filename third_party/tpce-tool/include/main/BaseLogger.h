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

#ifndef BASE_LOGGER_H
#define BASE_LOGGER_H

#include "utilities/EGenStandardTypes.h"
#include "DriverTypes.h"
#include "DriverParamSettings.h"
#include "BaseLogFormatter.h"

namespace TPCE {

/********************************* Generic Logger Class
 * ************************************/

class CBaseLogger {
private:
	char m_Prefix[64];
	CBaseLogFormatter *m_pLogFormatter;

	bool SendToLogger(const char *szPrefix, const char *szMsg);

protected:
	CBaseLogger(eDriverType drvType, INT32 UniqueId, CBaseLogFormatter *pFormatter);
	virtual bool SendToLoggerImpl(const char *szPrefix, const char *szTimestamp, const char *szMsg) = 0;

public:
	// Destructor
	virtual ~CBaseLogger() {
	}

	// Strings
	bool SendToLogger(const char *str);
	bool SendToLogger(string str);

	// Parameter Structures
	bool SendToLogger(CLoaderSettings &parms);
	bool SendToLogger(CDriverGlobalSettings &parms);
	bool SendToLogger(CDriverCESettings &parms);
	bool SendToLogger(CDriverCEPartitionSettings &parms);
	bool SendToLogger(CDriverMEESettings &parms);
	bool SendToLogger(CDriverDMSettings &parms);
	bool SendToLogger(CBrokerVolumeSettings &parms);
	bool SendToLogger(CCustomerPositionSettings &parms);
	bool SendToLogger(CMarketWatchSettings &parms);
	bool SendToLogger(CSecurityDetailSettings &parms);
	bool SendToLogger(CTradeLookupSettings &parms);
	bool SendToLogger(CTradeOrderSettings &parms);
	bool SendToLogger(CTradeUpdateSettings &parms);
	bool SendToLogger(CTxnMixGeneratorSettings &parms);
	bool SendToLogger(TDriverCETxnSettings &parms);
};

} // namespace TPCE

#endif // BASE_LOGGER_H
