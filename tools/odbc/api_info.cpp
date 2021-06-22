#include "duckdb_odbc.hpp"
#include "iostream"
using namespace duckdb;

template <idx_t N>
idx_t GetSize(SQLUSMALLINT (&array)[N]) {
	return N;
}

#define NUM_FUNC_SUPPORTED 4000

// fill all supported functions in this array
SQLUSMALLINT ALL_SUPPORTED_FUNCTIONS[] = {SQL_API_SQLALLOCHANDLE, SQL_API_SQLFREEHANDLE, SQL_API_SQLGETCONNECTATTR, SQL_API_SQLSETENVATTR,
										  SQL_API_SQLSETCONNECTATTR, SQL_API_SQLSETSTMTATTR, SQL_API_SQLDRIVERCONNECT, SQL_API_SQLCONNECT,
										  SQL_API_SQLGETINFO, SQL_API_SQLENDTRAN, SQL_API_SQLDISCONNECT, SQL_API_SQLTABLES, SQL_API_SQLCOLUMNS,
										  SQL_API_SQLPREPARE, SQL_API_SQLEXECDIRECT, SQL_API_SQLFREESTMT, SQL_API_SQLDESCRIBEPARAM,
										  SQL_API_SQLDESCRIBECOL, SQL_API_SQLCOLATTRIBUTES, SQL_API_SQLFETCHSCROLL, SQL_API_SQLROWCOUNT,
										  SQL_API_SQLGETDIAGFIELD, SQL_API_SQLGETDIAGREC, SQL_API_SQLGETFUNCTIONS
										 };

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-api-reference?view=sql-server-ver15

SQLRETURN GetFunctions(SQLHDBC ConnectionHandle, SQLUSMALLINT FunctionId, SQLUSMALLINT *SupportedPtr) {
	if (FunctionId == SQL_API_ALL_FUNCTIONS) {
		memset(SupportedPtr, SQL_FALSE, sizeof(SQLSMALLINT) * NUM_FUNC_SUPPORTED);
		idx_t func_idx;
		for(idx_t i=0; i < GetSize(ALL_SUPPORTED_FUNCTIONS); ++i) {
			func_idx = ALL_SUPPORTED_FUNCTIONS[i];
			SupportedPtr[func_idx] = SQL_TRUE;
		}

		return SQL_SUCCESS;
	}

	switch (FunctionId) {
	// handles
	case SQL_API_SQLALLOCHANDLE:
	case SQL_API_SQLFREEHANDLE:
	// attirbutes
	case SQL_API_SQLGETCONNECTATTR:
	case SQL_API_SQLSETENVATTR:
	case SQL_API_SQLSETCONNECTATTR:
	case SQL_API_SQLSETSTMTATTR:
	// connections
	case SQL_API_SQLDRIVERCONNECT:
	case SQL_API_SQLCONNECT:
	case SQL_API_SQLGETINFO:
	case SQL_API_SQLENDTRAN:
	case SQL_API_SQLDISCONNECT:
	// statements
	case SQL_API_SQLTABLES:
	case SQL_API_SQLCOLUMNS:
	case SQL_API_SQLPREPARE:
	case SQL_API_SQLEXECDIRECT:
	case SQL_API_SQLFREESTMT:
	case SQL_API_SQLDESCRIBEPARAM:
	case SQL_API_SQLDESCRIBECOL:
	case SQL_API_SQLCOLATTRIBUTES:
	case SQL_API_SQLFETCHSCROLL:
	case SQL_API_SQLROWCOUNT:
	// diagnostics
	case SQL_API_SQLGETDIAGFIELD:
	case SQL_API_SQLGETDIAGREC:
	// api info
	case SQL_API_SQLGETFUNCTIONS:
		*SupportedPtr = SQL_TRUE;
		break;
	default:
		*SupportedPtr = SQL_FALSE;
		break;
	}

	return SQL_SUCCESS;
}

#define SQL_FUNC_ESET(pfExists, uwAPI) \
		(*(((UWORD*) (pfExists)) + ((uwAPI) >> 4)) \
			|= (1 << ((uwAPI) & 0x000F)) \
				)
SQLRETURN GetFunctions30(SQLHDBC ConnectionHandle, SQLUSMALLINT FunctionId, SQLUSMALLINT *pfExists) {
	if (FunctionId != SQL_API_ODBC3_ALL_FUNCTIONS) {
		return SQL_ERROR;
	}
	memset(pfExists, 0, sizeof(UWORD) * SQL_API_ODBC3_ALL_FUNCTIONS_SIZE);

	// SQL_FUNC_ESET(pfExists, SQL_API_SQLBINDCOL);		/* 4 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLCANCEL);		/* 5 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLCOLATTRIBUTE);	/* 6 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLCONNECT);		/* 7 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLDESCRIBECOL);	/* 8 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLDISCONNECT);		/* 9 */

	SQL_FUNC_ESET(pfExists, SQL_API_SQLEXECDIRECT);		/* 11 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLEXECUTE);		/* 12 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLFETCH);			/* 13 */

	SQL_FUNC_ESET(pfExists, SQL_API_SQLFREESTMT);		/* 16 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLGETCURSORNAME);	/* 17 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLNUMRESULTCOLS);	/* 18 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLPREPARE);		/* 19 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLROWCOUNT);		/* 20 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLSETCURSORNAME);	/* 21 */

	SQL_FUNC_ESET(pfExists, SQL_API_SQLCOLUMNS);		/* 40 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLDRIVERCONNECT);	/* 41 */

	// SQL_FUNC_ESET(pfExists, SQL_API_SQLGETDATA);		/* 43 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLGETFUNCTIONS);	/* 44 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLGETINFO);		/* 45 */

	// SQL_FUNC_ESET(pfExists, SQL_API_SQLGETTYPEINFO);	/* 47 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLPARAMDATA);		/* 48 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLPUTDATA);		/* 49 */

	// SQL_FUNC_ESET(pfExists, SQL_API_SQLSPECIALCOLUMNS);	/* 52 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLSTATISTICS);		/* 53 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLTABLES);			/* 54 */

	// if (ci->drivers.lie)
	// 	SQL_FUNC_ESET(pfExists, SQL_API_SQLBROWSECONNECT);	/* 55 */
	// if (ci->drivers.lie)
	// 	SQL_FUNC_ESET(pfExists, SQL_API_SQLCOLUMNPRIVILEGES);	/* 56 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLDATASOURCES);	/* 57 */
	// if (SUPPORT_DESCRIBE_PARAM(ci) || ci->drivers.lie)
		SQL_FUNC_ESET(pfExists, SQL_API_SQLDESCRIBEPARAM); /* 58 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLEXTENDEDFETCH); /* 59 deprecated ? */

	// SQL_FUNC_ESET(pfExists, SQL_API_SQLFOREIGNKEYS);	/* 60 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLMORERESULTS);	/* 61 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLNATIVESQL);		/* 62 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLNUMPARAMS);		/* 63 */

	// SQL_FUNC_ESET(pfExists, SQL_API_SQLPRIMARYKEYS);	/* 65 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLPROCEDURECOLUMNS);	/* 66 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLPROCEDURES);			/* 67 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLSETPOS);			/* 68 */

	// SQL_FUNC_ESET(pfExists, SQL_API_SQLTABLEPRIVILEGES);	/* 70 */

	// SQL_FUNC_ESET(pfExists, SQL_API_SQLBINDPARAMETER);		/* 72 */

	SQL_FUNC_ESET(pfExists, SQL_API_SQLALLOCHANDLE);	/* 1001 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLBINDPARAM);		/* 1002 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLCLOSECURSOR);	/* 1003 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLCOPYDESC);		/* 1004 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLENDTRAN);		/* 1005 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLFREEHANDLE);		/* 1006 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLGETCONNECTATTR);	/* 1007 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLGETDESCFIELD);	/* 1008 */
	// if (ci->drivers.lie)
	// {
	// 	SQL_FUNC_ESET(pfExists, SQL_API_SQLGETDESCREC); /* 1009 not implemented yet */
	// }
	SQL_FUNC_ESET(pfExists, SQL_API_SQLGETDIAGFIELD); /* 1010 minimal implementation */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLGETDIAGREC);		/* 1011 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLGETENVATTR);		/* 1012 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLGETSTMTATTR);	/* 1014 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLSETCONNECTATTR);	/* 1016 */
	// SQL_FUNC_ESET(pfExists, SQL_API_SQLSETDESCFIELD);	/* 1017 */
	// if (ci->drivers.lie)
	// {
	// 	SQL_FUNC_ESET(pfExists, SQL_API_SQLSETDESCREC); /* 1018 not implemented yet */
	// }
	SQL_FUNC_ESET(pfExists, SQL_API_SQLSETENVATTR);		/* 1019 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLSETSTMTATTR);	/* 1020 */
	SQL_FUNC_ESET(pfExists, SQL_API_SQLFETCHSCROLL);	/* 1021 */
	// if (0 != (ALLOW_BULK_OPERATIONS & ci->updatable_cursors))
	// 	SQL_FUNC_ESET(pfExists, SQL_API_SQLBULKOPERATIONS);	/* 24 */

	return SQL_SUCCESS;
}


SQLRETURN SQLGetFunctions(SQLHDBC ConnectionHandle, SQLUSMALLINT FunctionId, SQLUSMALLINT *SupportedPtr) {
	if (FunctionId == SQL_API_ODBC3_ALL_FUNCTIONS) {
		return GetFunctions30(ConnectionHandle, FunctionId, SupportedPtr);
	}
	else {
		return GetFunctions(ConnectionHandle, FunctionId, SupportedPtr);
	}
}
