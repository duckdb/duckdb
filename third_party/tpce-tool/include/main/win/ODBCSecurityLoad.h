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
 *   Database loader class for SECURITY table.
 */
#ifndef ODBC_SECURITY_LOAD_H
#define ODBC_SECURITY_LOAD_H

namespace TPCE {

class CODBCSecurityLoad : public CDBLoader<SECURITY_ROW> {
	DBDATETIME ODBC_S_START_DATE;
	DBDATETIME ODBC_S_EXCH_DATE;
	DBDATETIME ODBC_S_52WK_LOW_DATE;
	DBDATETIME ODBC_S_52WK_HIGH_DATE;

	virtual inline void CopyRow(const SECURITY_ROW &row) {
		memcpy(&m_row, &row, sizeof(m_row));

		m_row.S_START_DATE.GetDBDATETIME(&ODBC_S_START_DATE);
		m_row.S_EXCH_DATE.GetDBDATETIME(&ODBC_S_EXCH_DATE);
		m_row.S_52WK_LOW_DATE.GetDBDATETIME(&ODBC_S_52WK_LOW_DATE);
		m_row.S_52WK_HIGH_DATE.GetDBDATETIME(&ODBC_S_52WK_HIGH_DATE);
	};

public:
	CODBCSecurityLoad(char *szServer, char *szDatabase, char *szLoaderParams, char *szTable = "SECURITY")
	    : CDBLoader<SECURITY_ROW>(szServer, szDatabase, szLoaderParams, szTable){};

	virtual void BindColumns() {
		// Binding function we have to implement.
		int i = 0;
		if ( // bcp_bind(m_hdbc, (BYTE *) &m_row.S_ID, 0, SQL_VARLEN_DATA, NULL,
		     // 0, SQLINT4, ++i) != SUCCEED
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_SYMB, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_ISSUE, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_ST_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_EX_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_CO_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_NUM_OUT, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT8, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&ODBC_S_START_DATE, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&ODBC_S_EXCH_DATE, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_PE, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_52WK_HIGH, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT4, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&ODBC_S_52WK_HIGH_DATE, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_52WK_LOW, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT4, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&ODBC_S_52WK_LOW_DATE, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_DIVIDEND, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.S_YIELD, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED)
			ThrowError(CODBCERR::eBcpBind);

		// if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK" ) != SUCCEED )
		//  ThrowError(CODBCERR::eBcpControl);
	};
};

} // namespace TPCE

#endif // ODBC_SECURITY_LOAD_H
