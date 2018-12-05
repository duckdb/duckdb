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
 * - Doug Johnson
 */

#include "main/ExchangeTable.h"

#include <cstring>

#include "utilities/MiscConsts.h"

using namespace TPCE;

void CExchangeTable::ComputeNumSecurities(TIdent iCustomerCount) {
	// There are 685 securities per Load Unit. The flat_in/Security.txt contains
	// 10 LUs worth of securities. This array is a pre-computation of the
	// cumulative number of securities each excahnge has in the first 10 LUs.
	const int iSecurityCounts[4][11] = {{0, 153, 307, 491, 688, 859, 1028, 1203, 1360, 1532, 1704},
	                                    {0, 173, 344, 498, 658, 848, 1006, 1191, 1402, 1572, 1749},
	                                    {0, 189, 360, 534, 714, 875, 1023, 1174, 1342, 1507, 1666},
	                                    {0, 170, 359, 532, 680, 843, 1053, 1227, 1376, 1554, 1731}};
	INT32 numLU = static_cast<INT32>(iCustomerCount / 1000);
	INT32 numLU_Tens = numLU / 10;
	INT32 numLU_Ones = numLU % 10;

	for (int i = 0; i < 4; i++) {
		securityCount[i] = iSecurityCounts[i][10] * numLU_Tens + iSecurityCounts[i][numLU_Ones];
	}
}

CExchangeTable::CExchangeTable(const ExchangeDataFile_t &dataFile, TIdent configuredCustomers)
    : FixedTable<ExchangeDataFile_t, EXCHANGE_ROW>(dataFile) {
	ComputeNumSecurities(configuredCustomers);
}

CExchangeTable::~CExchangeTable() {
}

void CExchangeTable::LoadTableRow() {
	const ExchangeDataFileRecord &dataRecord(df[recordIdx]);

	tableRow.EX_AD_ID = dataRecord.EX_AD_ID() + iTIdentShift;
	tableRow.EX_CLOSE = dataRecord.EX_CLOSE();
	strncpy(tableRow.EX_DESC, dataRecord.EX_DESC_CSTR(), sizeof(tableRow.EX_DESC));
	strncpy(tableRow.EX_ID, dataRecord.EX_ID_CSTR(), sizeof(tableRow.EX_ID));
	strncpy(tableRow.EX_NAME, dataRecord.EX_NAME_CSTR(), sizeof(tableRow.EX_NAME));
	tableRow.EX_NUM_SYMB = securityCount[recordIdx];
	tableRow.EX_OPEN = dataRecord.EX_OPEN();
}
