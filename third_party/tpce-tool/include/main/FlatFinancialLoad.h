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
 *   Flat file loader for FINANCIAL.
 */
#ifndef FLAT_FINANCIAL_LOAD_H
#define FLAT_FINANCIAL_LOAD_H

#include "FlatFileLoad_common.h"

namespace TPCE {

class CFlatFinancialLoad : public CFlatFileLoader<FINANCIAL_ROW> {
private:
	CDateTime Flat_FI_QTR_START_DATE;
	const std::string FinancialRowFmt;

public:
	CFlatFinancialLoad(char *szFileName, FlatFileOutputModes FlatFileOutputMode)
	    : CFlatFileLoader<FINANCIAL_ROW>(szFileName, FlatFileOutputMode),
	      FinancialRowFmt("%" PRId64 "|%d|%d|%s|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%" PRId64 "|%" PRId64 "\n"){};

	/*
	 *   Writes a record to the file.
	 */
	void WriteNextRecord(const FINANCIAL_ROW &next_record) {
		Flat_FI_QTR_START_DATE = next_record.FI_QTR_START_DATE;
		int rc = fprintf(hOutFile, FinancialRowFmt.c_str(), next_record.FI_CO_ID, next_record.FI_YEAR,
		                 next_record.FI_QTR, Flat_FI_QTR_START_DATE.ToStr(FlatFileDateFormat), next_record.FI_REVENUE,
		                 next_record.FI_NET_EARN, next_record.FI_BASIC_EPS, next_record.FI_DILUT_EPS,
		                 next_record.FI_MARGIN, next_record.FI_INVENTORY, next_record.FI_ASSETS,
		                 next_record.FI_LIABILITY, next_record.FI_OUT_BASIC, next_record.FI_OUT_DILUT);
		if (rc < 0) {
			throw CSystemErr(CSystemErr::eWriteFile, "CFlatFinancialLoad::WriteNextRecord");
		}
	}
};

} // namespace TPCE

#endif // FLAT_FINANCIAL_LOAD_H
