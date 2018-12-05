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

#ifndef WIN_ERROR_H
#define WIN_ERROR_H

#include "../utilities/error.h"

namespace TPCE {

#define ERR_TYPE_ODBC 6 // odbc generated error

class CODBCERR : public CBaseErr {
public:
	enum ACTION {
		eNone,
		eUnknown,
		eAllocConn,      // error from SQLAllocConnect
		eAllocHandle,    // error from SQLAllocHandle
		eBcpBind,        // error from bcp_bind
		eBcpControl,     // error from bcp_control
		eBcpInit,        // error from bcp_init
		eBcpBatch,       // error from bcp_batch
		eBcpDone,        // error from bcp_done
		eConnOption,     // error from SQLSetConnectOption
		eConnect,        // error from SQLConnect
		eAllocStmt,      // error from SQLAllocStmt
		eExecDirect,     // error from SQLExecDirect
		eBindParam,      // error from SQLBindParameter
		eBindCol,        // error from SQLBindCol
		eFetch,          // error from SQLFetch
		eFetchScroll,    // error from SQLFetchScroll
		eMoreResults,    // error from SQLMoreResults
		ePrepare,        // error from SQLPrepare
		eExecute,        // error from SQLExecute
		eBcpSendrow,     // error from bcp_sendrow
		eSetConnectAttr, // error from SQLSetConnectAttr
		eSetEnvAttr,     // error from SQLSetEnvAttr
		eSetStmtAttr,    // error from SQLSetStmtAttr
		eSetCursorName,  // error from SQLSetCursorName
		eSQLSetPos,      // error from SQLSetPos
		eEndTxn,         // error from SQLEndTxn
		eNumResultCols,  // error from SQLNumResultCols
		eCloseCursor,    // error from SQLCloseCursor
		eFreeStmt        // error from SQLFreeStmt
	};

	CODBCERR(char const *szLoc = "")
	    : CBaseErr(szLoc)

	{
		m_eAction = eNone;
		m_NativeError = 0;
		m_bDeadLock = false;
		m_odbcerrstr = NULL;
	};

	~CODBCERR() {
		if (m_odbcerrstr != NULL)
			delete[] m_odbcerrstr;
	};

	ACTION m_eAction;
	int m_NativeError;
	bool m_bDeadLock;
	char *m_odbcerrstr;

	int ErrorType() {
		return ERR_TYPE_ODBC;
	};
	int ErrorNum() {
		return m_NativeError;
	};
	const char *ErrorText() const {
		return m_odbcerrstr;
	};
};

} // namespace TPCE

#endif // WIN_ERROR_H
