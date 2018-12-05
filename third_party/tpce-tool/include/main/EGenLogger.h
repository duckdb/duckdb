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

#ifndef EGEN_LOGGER_H
#define EGEN_LOGGER_H

#include <sstream>
#include <fstream>
#include <cstring>

#include "utilities/EGenStandardTypes.h"
#include "DriverParamSettings.h"
#include "utilities/EGenVersion.h"
#include "BaseLogger.h"
#include "EGenLogFormatterTab.h"

namespace TPCE {

class CEGenLogger : public CBaseLogger {
private:
	char m_Filename[iMaxPath];
	ofstream m_Log;
	CMutex m_LogLock;

	bool SendToLoggerImpl(const char *szPrefix, const char *szTimestamp, const char *szMsg) {
		// m_LogLock.lock();
		// cerr << szPrefix << " " << szTimestamp << " " << szMsg << endl;
		// // m_Log.flush();
		// if (!m_Log)
		// {
		//     throw CSystemErr(CSystemErr::eWriteFile,
		//     "CEGenLogger::SendToLoggerImpl");
		// }
		// m_LogLock.unlock();
		return true;
	}

public:
	CEGenLogger(eDriverType drvType, UINT32 UniqueId, const char *szFilename, CBaseLogFormatter *pLogFormatter)
	    : CBaseLogger(drvType, UniqueId, pLogFormatter){
	          // Copy Log Filename
	          // strncpy(m_Filename, szFilename, sizeof(m_Filename));

	          // // Open Log File
	          // m_Log.open(m_Filename);
	          // if (!m_Log)
	          // {
	          //     throw CSystemErr(CSystemErr::eCreateFile,
	          //     "CEGenLogger::CEGenLogger");
	          // }
	      };
};

} // namespace TPCE

#endif // EGEN_LOGGER_H
