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
 *   Flat file loader for SECURITY.
 */
#ifndef FLAT_SECURITY_LOAD_H
#define FLAT_SECURITY_LOAD_H

#include "FlatFileLoad_common.h"

namespace TPCE {

class CFlatSecurityLoad : public CFlatFileLoader<SECURITY_ROW> {
private:
	CDateTime Flat_S_START_DATE;
	CDateTime Flat_S_EXCH_DATE;
	CDateTime Flat_S_52WK_HIGH_DATE;
	CDateTime Flat_S_52WK_LOW_DATE;
	const std::string SecurityRowFmt;

public:
	CFlatSecurityLoad(char *szFileName, FlatFileOutputModes FlatFileOutputMode)
	    : CFlatFileLoader<SECURITY_ROW>(szFileName, FlatFileOutputMode),
	      SecurityRowFmt("%s|%s|%s|%s|%s|%" PRId64 "|%" PRId64 "|%s|%s|%.2f|%.2f|%s|%.2f|%s|%.2f|%.2f\n"){};

	/*
	 *   Writes a record to the file.
	 */
	void WriteNextRecord(const SECURITY_ROW &next_record) {
		Flat_S_START_DATE = next_record.S_START_DATE;
		Flat_S_EXCH_DATE = next_record.S_EXCH_DATE;
		Flat_S_52WK_HIGH_DATE = next_record.S_52WK_HIGH_DATE;
		Flat_S_52WK_LOW_DATE = next_record.S_52WK_LOW_DATE;
		int rc = fprintf(hOutFile, SecurityRowFmt.c_str(), next_record.S_SYMB, next_record.S_ISSUE, next_record.S_ST_ID,
		                 next_record.S_NAME, next_record.S_EX_ID, next_record.S_CO_ID, next_record.S_NUM_OUT,
		                 Flat_S_START_DATE.ToStr(FlatFileDateFormat), Flat_S_EXCH_DATE.ToStr(FlatFileDateFormat),
		                 next_record.S_PE, next_record.S_52WK_HIGH, Flat_S_52WK_HIGH_DATE.ToStr(FlatFileDateFormat),
		                 next_record.S_52WK_LOW, Flat_S_52WK_LOW_DATE.ToStr(FlatFileDateFormat), next_record.S_DIVIDEND,
		                 next_record.S_YIELD);

		if (rc < 0) {
			throw CSystemErr(CSystemErr::eWriteFile, "CFlatSecurityLoad::WriteNextRecord");
		}
	}
};

} // namespace TPCE

#endif // FLAT_SECURITY_LOAD_H
