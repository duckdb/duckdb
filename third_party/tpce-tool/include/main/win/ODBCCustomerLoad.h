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
 *   Database loader class for CUSTOMER table.
 */
#ifndef ODBC_CUSTOMER_LOAD_H
#define ODBC_CUSTOMER_LOAD_H

namespace TPCE {

class CODBCCustomerLoad : public CDBLoader<CUSTOMER_ROW> {
private:
	DBDATETIME ODBC_C_DOB;
	virtual inline void CopyRow(const CUSTOMER_ROW &row) {
		memcpy(&m_row, &row, sizeof(m_row));
		m_row.C_DOB.GetDBDATETIME(&ODBC_C_DOB);
	};

public:
	CODBCCustomerLoad(char *szServer, char *szDatabase, char *szLoaderParams, char *szTable = "CUSTOMER")
	    : CDBLoader<CUSTOMER_ROW>(szServer, szDatabase, szLoaderParams, szTable){};

	virtual void BindColumns() {
		int i = 0;
		if (bcp_bind(m_hdbc, (BYTE *)&m_row.C_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_TAX_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_ST_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_L_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_F_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_M_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_GNDR, 0, 1, NULL, 0, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_TIER, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT1, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&ODBC_C_DOB, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_AD_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_CTRY_1, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_AREA_1, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_LOCAL_1, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_EXT_1, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_CTRY_2, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_AREA_2, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_LOCAL_2, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_EXT_2, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_CTRY_3, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_AREA_3, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_LOCAL_3, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_EXT_3, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_EMAIL_1, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) !=
		        SUCCEED ||
		    bcp_bind(m_hdbc, (BYTE *)&m_row.C_EMAIL_2, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED)
			ThrowError(CODBCERR::eBcpBind);

		if (bcp_control(m_hdbc, BCPHINTS, "ORDER (C_ID)") != SUCCEED)
			ThrowError(CODBCERR::eBcpControl);
	}
};

} // namespace TPCE

#endif // ODBC_CUSTOMER_LOAD_H
