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
 *    Description:        This file implements the interface for
 *                        formatting logger entries.
 ******************************************************************************/

#ifndef BASE_LOG_FORMATTER_H
#define BASE_LOG_FORMATTER_H

#include <string>
#include "DriverParamSettings.h"

namespace TPCE {

enum eLogFormat { eLogTab, eLogCustom };

class CBaseLogFormatter {
public:
	/*
	 *  Virtual destructor. Provided so that a sponsor-specific
	 *  destructor can be called on destruction from the base-class pointer.
	 *
	 *  PARAMETERS:
	 *           none.
	 *
	 *  RETURNS:
	 *           not applicable.
	 */
	virtual ~CBaseLogFormatter(){};

	virtual string GetLogOutput(CBrokerVolumeSettings &parms) = 0;
	virtual string GetLogOutput(CCustomerPositionSettings &parms) = 0;
	virtual string GetLogOutput(CMarketWatchSettings &parms) = 0;
	virtual string GetLogOutput(CSecurityDetailSettings &parms) = 0;
	virtual string GetLogOutput(CTradeLookupSettings &parms) = 0;
	virtual string GetLogOutput(CTradeOrderSettings &parms) = 0;
	virtual string GetLogOutput(CTradeUpdateSettings &parms) = 0;
	virtual string GetLogOutput(CTxnMixGeneratorSettings &parms) = 0;
	virtual string GetLogOutput(CLoaderSettings &parms) = 0;
	virtual string GetLogOutput(CDriverGlobalSettings &parms) = 0;
	virtual string GetLogOutput(CDriverCESettings &parms) = 0;
	virtual string GetLogOutput(CDriverCEPartitionSettings &parms) = 0;
	virtual string GetLogOutput(CDriverMEESettings &parms) = 0;
	virtual string GetLogOutput(CDriverDMSettings &parms) = 0;
};

} // namespace TPCE

#endif // BASE_LOG_FORMATTER_H
