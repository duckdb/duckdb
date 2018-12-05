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
 *   Flat file loader for EXCHANGE.
 */
#ifndef FLAT_EXCHANGE_LOAD_H
#define FLAT_EXCHANGE_LOAD_H

#include "FlatFileLoad_common.h"

namespace TPCE {

class CFlatExchangeLoad : public CFlatFileLoader<EXCHANGE_ROW> {
private:
	const std::string ExchangeRowFmt;

public:
	CFlatExchangeLoad(char *szFileName, FlatFileOutputModes FlatFileOutputMode)
	    : CFlatFileLoader<EXCHANGE_ROW>(szFileName, FlatFileOutputMode),
	      ExchangeRowFmt("%s|%s|%d|%d|%d|%s|%" PRId64 "\n"){};

	/*
	 *   Writes a record to the file.
	 */
	void WriteNextRecord(const EXCHANGE_ROW &next_record) {
		int rc =
		    fprintf(hOutFile, ExchangeRowFmt.c_str(), next_record.EX_ID, next_record.EX_NAME, next_record.EX_NUM_SYMB,
		            next_record.EX_OPEN, next_record.EX_CLOSE, next_record.EX_DESC, next_record.EX_AD_ID);

		if (rc < 0) {
			throw CSystemErr(CSystemErr::eWriteFile, "CFlatExchangeLoad::WriteNextRecord");
		}
	}
};

} // namespace TPCE

#endif // FLAT_EXCHANGE_LOAD_H
