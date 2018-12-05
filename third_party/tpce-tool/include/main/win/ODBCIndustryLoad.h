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
 *   Database loader class for INDUSTRY table.
 */
#ifndef ODBC_INDUSTRY_LOAD_H
#define ODBC_INDUSTRY_LOAD_H

#include "../utilities/TableConsts.h"

namespace TPCE {

class CODBCIndustryLoad : public CDBLoader<INDUSTRY_ROW> {
public:
	CODBCIndustryLoad(char *szServer, char *szDatabase, char *szLoaderParams, char *szTable = "INDUSTRY")
	    : CDBLoader<INDUSTRY_ROW>(szServer, szDatabase, szLoaderParams, szTable){};

	virtual void BindColumns() {
		// Binding function we have to implement.
		int i = 0;
		if (bcp_bind(m_hdbc, (BYTE *)&m_row.IN_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.IN_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.IN_SC_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED)
			ThrowError(CODBCERR::eBcpBind);

		//      if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK" ) != SUCCEED )
		//          ThrowError(CODBCERR::eBcpControl);
	};

	virtual void CopyRow(const INDUSTRY_ROW &row) {
		strncpy(m_row.IN_ID, row.IN_ID, sizeof(m_row.IN_ID));
		strncpy(m_row.IN_NAME, row.IN_NAME, sizeof(m_row.IN_NAME));
		strncpy(m_row.IN_SC_ID, row.IN_SC_ID, sizeof(m_row.IN_SC_ID));
	};
};

} // namespace TPCE

#endif // ODBC_INDUSTRY_LOAD_H
