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

/*
 *   Flat file loader for CUSTOMER_ACCOUNT.
 */
#ifndef FLAT_CUSTOMER_ACCOUNT_LOAD_H
#define FLAT_CUSTOMER_ACCOUNT_LOAD_H

#include "FlatFileLoad_common.h"

namespace TPCE {

class CFlatCustomerAccountLoad : public CFlatFileLoader<CUSTOMER_ACCOUNT_ROW> {
private:
	const std::string CustomerAccountRowFmt;

public:
	CFlatCustomerAccountLoad(char *szFileName, FlatFileOutputModes FlatFileOutputMode)
	    : CFlatFileLoader<CUSTOMER_ACCOUNT_ROW>(szFileName, FlatFileOutputMode),
	      CustomerAccountRowFmt("%" PRId64 "|%" PRId64 "|%" PRId64 "|%s|%d|%.2f\n"){};

	/*
	 *   Writes a record to the file.
	 */
	void WriteNextRecord(const CUSTOMER_ACCOUNT_ROW &next_record) {
		int rc = fprintf(hOutFile, CustomerAccountRowFmt.c_str(), next_record.CA_ID, next_record.CA_B_ID,
		                 next_record.CA_C_ID, next_record.CA_NAME, (int)next_record.CA_TAX_ST, next_record.CA_BAL);
		if (rc < 0) {
			throw CSystemErr(CSystemErr::eWriteFile, "CFlatCustomerAccountLoad::WriteNextRecord");
		}
	}
};

} // namespace TPCE

#endif // FLAT_CUSTOMER_ACCOUNT_LOAD_H
