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
 * - Sergey Vasilevskiy
 */

/*
 *   Database loader class for COMPANY table.
 */
#ifndef ODBC_COMPANY_LOAD_H
#define ODBC_COMPANY_LOAD_H

namespace TPCE {

class CODBCCompanyLoad : public CDBLoader<COMPANY_ROW> {
private:
	DBDATETIME ODBC_CO_OPEN_DATE;
	virtual inline void CopyRow(const COMPANY_ROW &row) {
		memcpy(&m_row, &row, sizeof(m_row));
		m_row.CO_OPEN_DATE.GetDBDATETIME(&ODBC_CO_OPEN_DATE);
	};

public:
	CODBCCompanyLoad(char *szServer, char *szDatabase, char *szLoaderParams, char *szTable = "COMPANY")
	    : CDBLoader<COMPANY_ROW>(szServer, szDatabase, szLoaderParams, szTable){};

	virtual void BindColumns() {
		// Binding function we have to implement.
		int i = 0;
		if (bcp_bind(m_hdbc, (BYTE *)&m_row.CO_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.CO_ST_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.CO_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.CO_IN_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.CO_SP_RATE, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.CO_CEO, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.CO_AD_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.CO_DESC, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&ODBC_CO_OPEN_DATE, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED)
			ThrowError(CODBCERR::eBcpBind);

		if (bcp_control(m_hdbc, BCPHINTS, "ORDER (CO_ID)") != SUCCEED)
			ThrowError(CODBCERR::eBcpControl);
	};
};

} // namespace TPCE

#endif // ODBC_COMPANY_LOAD_H
