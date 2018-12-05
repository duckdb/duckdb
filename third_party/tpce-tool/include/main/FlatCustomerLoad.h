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
 *   Flat file loader for CUSTOMER.
 */

#ifndef FLAT_CUSTOMER_LOAD_H
#define FLAT_CUSTOMER_LOAD_H

#include "FlatFileLoad_common.h"

namespace TPCE {

class CFlatCustomerLoad : public CFlatFileLoader<CUSTOMER_ROW> {
private:
	CDateTime Flat_C_DOB;
	const std::string CustomerRowFmt;

public:
	CFlatCustomerLoad(char *szFileName, FlatFileOutputModes FlatFileOutputMode)
	    : CFlatFileLoader<CUSTOMER_ROW>(szFileName, FlatFileOutputMode),
	      CustomerRowFmt("%" PRId64 "|%s|%s|%s|%s|%s|%c|%d|%s|%" PRId64
	                     "|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n"){};

	/*
	 *   Writes a record to the file.
	 */
	void WriteNextRecord(const CUSTOMER_ROW &next_record) {
		Flat_C_DOB = next_record.C_DOB;
		int rc = fprintf(hOutFile, CustomerRowFmt.c_str(), next_record.C_ID, next_record.C_TAX_ID, next_record.C_ST_ID,
		                 next_record.C_L_NAME, next_record.C_F_NAME, next_record.C_M_NAME, next_record.C_GNDR,
		                 (int)next_record.C_TIER, Flat_C_DOB.ToStr(FlatFileDateFormat), next_record.C_AD_ID,
		                 next_record.C_CTRY_1, next_record.C_AREA_1, next_record.C_LOCAL_1, next_record.C_EXT_1,
		                 next_record.C_CTRY_2, next_record.C_AREA_2, next_record.C_LOCAL_2, next_record.C_EXT_2,
		                 next_record.C_CTRY_3, next_record.C_AREA_3, next_record.C_LOCAL_3, next_record.C_EXT_3,
		                 next_record.C_EMAIL_1, next_record.C_EMAIL_2);
		if (rc < 0) {
			throw CSystemErr(CSystemErr::eWriteFile, "CFlatCustomerLoad::WriteNextRecord");
		}
	}
};

} // namespace TPCE

#endif // FLAT_CUSTOMER_LOAD_H
