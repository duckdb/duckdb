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
 *   Class representing database bulk loader.
 */
#ifndef DB_LOADER_H
#define DB_LOADER_H

#include "ODBCLoad_stdafx.h"

namespace TPCE {

// Database binding type for integer identifiers (IDENT_T metatype in TPC-E
// spec).
//
#define IDENT_BIND SQLINT8

// For nullable columns
typedef struct BCPDOUBLE {
	int iIndicator;
	float value;
} * PBCPDOUBLE;
typedef struct BCPDBDATETIME {
	int iIndicator;
	DBDATETIME value;
} * PBCPDBDATETIME;

/*
 *   DBLoader class.
 */
template <typename T> class CDBLoader : public CBaseLoader<T> {
protected:
	T m_row;
	int m_cnt;
	SQLHENV m_henv; // ODBC environment handle
	SQLHDBC m_hdbc;
	SQLHSTMT m_hstmt;                // the current hstmt
	char m_szServer[iMaxHostname];   // server name
	char m_szDatabase[iMaxDBName];   // name of the database being loaded
	char m_szLoaderParams[iMaxPath]; // loader parameters specified on the
	                                 // command-line
	char m_szTable[iMaxPath];        // name of the table being loaded

	virtual void CopyRow(const T &row) {
		memcpy(&m_row, &row, sizeof(m_row));
	};

public:
	CDBLoader(char *szServer, char *szDatabase, char *szLoaderParams, char *szTable);
	~CDBLoader(void);

	virtual void BindColumns() = 0; // column binding function subclasses must implement
	virtual void Init();            // resets to clean state; needed after FinishLoad to
	                                // continue loading
	virtual void Commit();          // commit rows sent so far
	virtual void FinishLoad();      // finish load
	void Connect();                 // connect to SQL Server
	void Disconnect();              // disconnect - should not throw any exceptions (to put
	                                // into the destructor)

	void ThrowError(CODBCERR::ACTION eAction, SQLSMALLINT HandleType = 0, SQLHANDLE Handle = SQL_NULL_HANDLE);
	virtual void WriteNextRecord(const T &next_record);
};

} // namespace TPCE

#endif // DB_LOADER_H
