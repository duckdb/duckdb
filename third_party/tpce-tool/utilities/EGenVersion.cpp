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
 * - Sergey Vasilevskiy, Doug Johnson, Matt Emmerton
 */

/******************************************************************************
 *   Description:        Versioning information for the EGen package.
 *                       Updated on every release.
 ******************************************************************************/

#include "utilities/EGenVersion.h"

#include <stdio.h>
#include <cstring>

namespace TPCE {

// Modify these constants whenever EGen version changes.
//
static INT32 iEGenMajorVersion = 1;   // major revision number
static INT32 iEGenMinorVersion = 14;  // minor revision number
static INT32 iEGenRevisionNumber = 0; // third-tier revision number
static INT32 iEGenBetaLevel = 0;      // beta version (for maintenance only)

extern "C" {
void GetEGenVersion_C(INT32 &iMajorVersion, INT32 &iMinorVersion, INT32 &iRevisionNumber, INT32 &iBetaLevel) {
	GetEGenVersion(iMajorVersion, iMinorVersion, iRevisionNumber, iBetaLevel);
}

void GetEGenVersionString_C(char *szOutput, size_t iOutputBufferLen) {
	GetEGenVersionString(szOutput, iOutputBufferLen);
}

void PrintEGenVersion_C() {
	PrintEGenVersion();
}

void GetEGenVersionUpdateTimestamp_C(char *szOutput, size_t iOutputBufferLen) {
	GetEGenVersionUpdateTimestamp(szOutput, iOutputBufferLen);
}

} // extern "C"

// Retrieve major, minor, revision, and beta level numbers for EGen.
//
void GetEGenVersion(INT32 &iMajorVersion, INT32 &iMinorVersion, INT32 &iRevisionNumber, INT32 &iBetaLevel) {
	iMajorVersion = iEGenMajorVersion;
	iMinorVersion = iEGenMinorVersion;
	iRevisionNumber = iEGenRevisionNumber;
	iBetaLevel = iEGenBetaLevel;
}

// Return versioning information formated as a string
//
void GetEGenVersionString(char *szOutput, size_t iOutputBufferLen) {
	if (iEGenBetaLevel == 0) {
		snprintf(szOutput, iOutputBufferLen, "EGen v%d.%d.%d", iEGenMajorVersion, iEGenMinorVersion,
		         iEGenRevisionNumber);
	} else {
		snprintf(szOutput, iOutputBufferLen, "EGen v%d.%d.%d beta %d", iEGenMajorVersion, iEGenMinorVersion,
		         iEGenRevisionNumber, iEGenBetaLevel);
	}
}

// Output EGen versioning information on stdout
//
void PrintEGenVersion() {
	char szVersion[33];

	GetEGenVersionString(szVersion, sizeof(szVersion));

	printf("%s\n", szVersion);
}

// Return the date/time when the EGen versioning information was last updated.
//
void GetEGenVersionUpdateTimestamp(char *szOutput, size_t iOutputBufferLen) {
	std::strncpy(szOutput, __DATE__, iOutputBufferLen);
}

} // namespace TPCE
