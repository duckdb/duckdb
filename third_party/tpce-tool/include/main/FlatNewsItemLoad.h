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
 *   Flat file loader for NEWS_ITEM.
 */
#ifndef FLAT_NEWS_ITEM_LOAD_H
#define FLAT_NEWS_ITEM_LOAD_H

#include "FlatFileLoad_common.h"

namespace TPCE {

class CFlatNewsItemLoad : public CFlatFileLoader<NEWS_ITEM_ROW> {
private:
	CDateTime Flat_NI_DTS;
	const std::string NewsItemRowFmt;

public:
	CFlatNewsItemLoad(char *szFileName, FlatFileOutputModes FlatFileOutputMode)
	    : CFlatFileLoader<NEWS_ITEM_ROW>(szFileName, FlatFileOutputMode),
	      NewsItemRowFmt("%" PRId64 "|%s|%s|%s|%s|%s|%s\n"){};

	/*
	 *   Writes a record to the file.
	 */
	void WriteNextRecord(const NEWS_ITEM_ROW &next_record) {
		Flat_NI_DTS = next_record.NI_DTS;
		int rc = fprintf(hOutFile, NewsItemRowFmt.c_str(), next_record.NI_ID, next_record.NI_HEADLINE,
		                 next_record.NI_SUMMARY, next_record.NI_ITEM, Flat_NI_DTS.ToStr(FlatFileDateTimeFormat),
		                 next_record.NI_SOURCE, next_record.NI_AUTHOR);
		if (rc < 0) {
			throw CSystemErr(CSystemErr::eWriteFile, "CFlatNewsItemLoad::WriteNextRecord");
		}
	}
};

} // namespace TPCE

#endif // FLAT_NEWS_ITEM_LOAD_H
