// needs to be first because BOOL
#include "duckdb.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include <assert.h>

namespace duckdb {
enum OdbcHandleType { ENV, DBC, STMT };
struct OdbcHandle {
	OdbcHandle(OdbcHandleType type_p) : type(type_p) {};
	OdbcHandleType type;
};

struct OdbcHandleEnv : public OdbcHandle {
	OdbcHandleEnv() : OdbcHandle(OdbcHandleType::ENV), db(make_unique<DuckDB>(nullptr)) {};
	unique_ptr<DuckDB> db;
};

struct OdbcHandleDbc : public OdbcHandle {
	OdbcHandleDbc(OdbcHandleEnv *env_p) : OdbcHandle(OdbcHandleType::DBC), env(env_p) {
		assert(env_p);
		assert(env_p->db);
	};
	OdbcHandleEnv *env;
	unique_ptr<Connection> conn;
};

struct OdbcBoundCol {
	OdbcBoundCol() : type(SQL_UNKNOWN_TYPE), ptr(nullptr), len(0), strlen_or_ind(nullptr) {};

	bool IsBound() {
		return ptr != nullptr;
	}

	SQLSMALLINT type;
	SQLPOINTER ptr;
	SQLLEN len;
	SQLLEN *strlen_or_ind;
};

struct OdbcHandleStmt : public OdbcHandle {
	OdbcHandleStmt(OdbcHandleDbc *dbc_p) : OdbcHandle(OdbcHandleType::STMT), dbc(dbc_p), rows_fetched_ptr(nullptr) {
		assert(dbc_p);
		assert(dbc_p->conn);
	};

	OdbcHandleDbc *dbc;
	unique_ptr<PreparedStatement> stmt;
	unique_ptr<QueryResult> res;
	unique_ptr<DataChunk> chunk;
	vector<Value> params;
	vector<OdbcBoundCol> bound_cols;
	bool open;
	row_t chunk_row;
	SQLULEN *rows_fetched_ptr;
};

template <class T>
SQLRETURN WithStatement(SQLHANDLE &StatementHandle, T &&lambda) {
	if (!StatementHandle) {
		return SQL_ERROR;
	}
	auto *hdl = (OdbcHandleStmt *)StatementHandle;
	if (hdl->type != OdbcHandleType::STMT) {
		return SQL_ERROR;
	}
	if (!hdl->dbc || !hdl->dbc->conn) {
		return SQL_ERROR;
	}

	return lambda(hdl);
}

template <class T>
SQLRETURN WithStatementPrepared(SQLHANDLE &StatementHandle, T &&lambda) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!stmt->stmt) {
			return SQL_ERROR;
		}
		if (!stmt->stmt->success) {
			return SQL_ERROR;
		}
		return lambda(stmt);
	});
}

template <class T>
SQLRETURN WithStatementResult(SQLHANDLE &StatementHandle, T &&lambda) {
	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!stmt->res) {
			return SQL_ERROR;
		}
		if (!stmt->res->success) {
			return SQL_ERROR;
		}
		return lambda(stmt);
	});
}

} // namespace duckdb

using namespace duckdb;

extern "C" {
SQLRETURN SQLAllocHandle(SQLSMALLINT HandleType, SQLHANDLE InputHandle, SQLHANDLE *OutputHandlePtr) {

	switch (HandleType) {
	case SQL_HANDLE_DBC: {
		assert(InputHandle);
		auto *env = (OdbcHandleEnv *)InputHandle;
		assert(env->type == OdbcHandleType::ENV);
		*OutputHandlePtr = new OdbcHandleDbc(env);
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC:
		throw std::runtime_error("SQL_HANDLE_DESC");
	case SQL_HANDLE_ENV:
		*OutputHandlePtr = new OdbcHandleEnv();
		return SQL_SUCCESS;
	case SQL_HANDLE_STMT: {
		assert(InputHandle);
		auto *dbc = (OdbcHandleDbc *)InputHandle;
		assert(dbc->type == OdbcHandleType::DBC);
		*OutputHandlePtr = new OdbcHandleStmt(dbc);
		return SQL_SUCCESS;
		break;
	}
	default:
		return SQL_ERROR;
	}
	return SQL_ERROR;
}

SQLRETURN SQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength) {
	return SQL_SUCCESS; // TODO
}

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function
SQLRETURN SQLSetConnectAttr(SQLHDBC ConnectionHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr,
                            SQLINTEGER StringLength) {

	if (!ConnectionHandle) {
		return SQL_ERROR;
	}
	auto *hdl = (OdbcHandleDbc *)ConnectionHandle;
	if (hdl->type != OdbcHandleType::DBC) {
		return SQL_ERROR;
	}
	if (!hdl->conn) {
		return SQL_ERROR;
	}
	switch (Attribute) {
	case SQL_ATTR_AUTOCOMMIT:
		// assert StringLength == SQL_IS_UINTEGER
		switch ((ptrdiff_t)ValuePtr) {
		case (ptrdiff_t)SQL_AUTOCOMMIT_ON:
			hdl->conn->SetAutoCommit(true);
			break;
		case (ptrdiff_t)SQL_AUTOCOMMIT_OFF:
			hdl->conn->SetAutoCommit(false);
			break;
		default:
			return SQL_ERROR;
		}
		break;
	default:
		return SQL_ERROR;
	}

	return SQL_SUCCESS; // TODO
}

SQLRETURN SQLSetStmtAttr(SQLHSTMT StatementHandle, SQLINTEGER Attribute, SQLPOINTER ValuePtr, SQLINTEGER StringLength) {

	if (!StatementHandle || !ValuePtr) {
		return SQL_ERROR;
	}

	auto stmt = (OdbcHandleStmt *)StatementHandle;
	if (stmt->type != OdbcHandleType::STMT) {
		return SQL_ERROR;
	}

	switch (Attribute) {
	case SQL_ATTR_PARAMSET_SIZE: {
		/* auto size = Load<SQLLEN>((data_ptr_t) ValuePtr);
		 return (size == 1) ? SQL_SUCCESS : SQL_ERROR;
		 */
		// this should be 1
		return SQL_SUCCESS;
	}
	case SQL_ATTR_QUERY_TIMEOUT: {
		// this should be 0
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROW_ARRAY_SIZE: {
		// this should be 1 (for now!)
		// TODO allow fetch to put more rows in bound cols
		auto new_size = (SQLULEN)ValuePtr;
		if (new_size != 1) {
			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	}
	case SQL_ATTR_ROWS_FETCHED_PTR: {
		stmt->rows_fetched_ptr = (SQLULEN *)ValuePtr;
		return SQL_SUCCESS;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLEndTran(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT CompletionType) {
	if (HandleType != SQL_HANDLE_DBC) { // theoretically this can also be done on env but no no no
		return SQL_ERROR;
	}
	auto *dbc = (OdbcHandleDbc *)Handle;
	assert(dbc->type == OdbcHandleType::DBC);
	switch (CompletionType) {
	case SQL_COMMIT:
		dbc->conn->Commit();
	case SQL_ROLLBACK:
		dbc->conn->Rollback();
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLDriverConnect(SQLHDBC ConnectionHandle, SQLHWND WindowHandle, SQLCHAR *InConnectionString,
                           SQLSMALLINT StringLength1, SQLCHAR *OutConnectionString, SQLSMALLINT BufferLength,
                           SQLSMALLINT *StringLength2Ptr, SQLUSMALLINT DriverCompletion) {
	// TODO actually interpret Database in InConnectionString
	auto *dbc = (OdbcHandleDbc *)ConnectionHandle;
	assert(dbc->type == OdbcHandleType::DBC);
	if (!dbc->conn) {
		dbc->conn = make_unique<Connection>(*dbc->env->db);
	}
	return SQL_SUCCESS;
}

SQLRETURN SQLFreeHandle(SQLSMALLINT HandleType, SQLHANDLE Handle) {

	if (!Handle) {
		return SQL_ERROR;
	}

	switch (HandleType) {
	case SQL_HANDLE_DBC: {
		auto *hdl = (OdbcHandleDbc *)Handle;
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC:
		throw std::runtime_error("SQL_HANDLE_DESC");
	case SQL_HANDLE_ENV: {
		auto *hdl = (OdbcHandleEnv *)Handle;
		delete hdl;
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_STMT: {
		auto *hdl = (OdbcHandleStmt *)Handle;
		delete hdl;
		return SQL_SUCCESS;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (Option != SQL_CLOSE) {
			return SQL_ERROR;
		}
		stmt->res.reset();
		stmt->chunk.reset();
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLDisconnect(SQLHDBC ConnectionHandle) {

	if (!ConnectionHandle) {
		return SQL_ERROR;
	}
	auto *dbc = (OdbcHandleDbc *)ConnectionHandle;
	if (dbc->type != OdbcHandleType::DBC) {
		return SQL_ERROR;
	}
	if (!dbc->conn) {
		return SQL_ERROR;
	}
	dbc->conn.reset();
	return SQL_SUCCESS;
}

SQLRETURN SQLExecDirect(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength) {
	auto prepare_status = SQLPrepare(StatementHandle, StatementText, TextLength);
	if (prepare_status != SQL_SUCCESS) {
		return SQL_ERROR;
	}
	auto execute_status = SQLExecute(StatementHandle);
	if (execute_status != SQL_SUCCESS) {
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
}

SQLRETURN SQLPrepare(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
		stmt->stmt.reset();
		stmt->res.reset();
		stmt->chunk.reset();
		stmt->params.resize(0);
		stmt->bound_cols.resize(0);

		string query = TextLength == SQL_NTS ? string((const char *)StatementText)
		                                     : string((const char *)StatementText, (size_t)TextLength);
		stmt->stmt = stmt->dbc->conn->Prepare(query);
		if (!stmt->stmt->success) {
			return SQL_ERROR;
		}
		stmt->params.resize(stmt->stmt->n_param);
		stmt->bound_cols.resize(stmt->stmt->ColumnCount());
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLBindParameter(SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber, SQLSMALLINT InputOutputType,
                           SQLSMALLINT ValueType, SQLSMALLINT ParameterType, SQLULEN ColumnSize,
                           SQLSMALLINT DecimalDigits, SQLPOINTER ParameterValuePtr, SQLLEN BufferLength,
                           SQLLEN *StrLen_or_IndPtr) {
	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (ParameterNumber > stmt->stmt->n_param) {
			return SQL_ERROR;
		}
		if (InputOutputType != SQL_PARAM_INPUT) {
			return SQL_ERROR;
		}

		// it would appear that the ParameterType does not matter that much
		// we cast it anyway and if the cast fails we will hear about it during execution
		Value res;
		auto dataptr = (const_data_ptr_t)ParameterValuePtr;
		switch (ValueType) {
		case SQL_C_CHAR: {
			// TODO this copies twice for now
			string param = BufferLength == SQL_NTS ? string((const char *)ParameterValuePtr)
			                                       : string((const char *)ParameterValuePtr, (size_t)BufferLength);
			res = Value(param);
			break;
		}
		case SQL_C_TINYINT:
		case SQL_C_STINYINT:
			res = Value::TINYINT(Load<int8_t>(dataptr));
			break;
		case SQL_C_UTINYINT:
			res = Value::TINYINT(Load<uint8_t>(dataptr));
			break;
		case SQL_C_SHORT:
		case SQL_C_SSHORT:
			res = Value::SMALLINT(Load<int16_t>(dataptr));
			break;
		case SQL_C_USHORT:
			res = Value::USMALLINT(Load<uint16_t>(dataptr));
			break;
		case SQL_C_SLONG:
		case SQL_C_LONG:
			res = Value::INTEGER(Load<int32_t>(dataptr));
			break;
		case SQL_C_ULONG:
			res = Value::UINTEGER(Load<uint32_t>(dataptr));
			break;
		case SQL_C_SBIGINT:
			res = Value::BIGINT(Load<int64_t>(dataptr));
			break;
		case SQL_C_UBIGINT:
			res = Value::UBIGINT(Load<uint64_t>(dataptr));
			break;
		case SQL_C_FLOAT:
			res = Value::FLOAT(Load<float>(dataptr));
			break;
		case SQL_C_DOUBLE:
			res = Value::DOUBLE(Load<double>(dataptr));
			break;

			// TODO moar types

		default:
			// TODO error message?
			return SQL_ERROR;
		}
		stmt->params[ParameterNumber - 1] = res;
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLExecute(SQLHSTMT StatementHandle) {
	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		stmt->res.reset();
		stmt->chunk.reset();
		stmt->chunk_row = -1;
		stmt->open = false;
		if (stmt->rows_fetched_ptr) {
			*stmt->rows_fetched_ptr = 0;
		}
		stmt->res = stmt->stmt->Execute(stmt->params);
		if (!stmt->res->success) {
			return SQL_ERROR;
		}
		stmt->open = true;
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLNumResultCols(SQLHSTMT StatementHandle, SQLSMALLINT *ColumnCountPtr) {
	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!ColumnCountPtr) {
			return SQL_ERROR;
		}
		*ColumnCountPtr = (SQLSMALLINT)stmt->stmt->GetTypes().size();
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLNumParams(SQLHSTMT StatementHandle, SQLSMALLINT *ParameterCountPtr) {
	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!ParameterCountPtr) {
			return SQL_ERROR;
		}
		*ParameterCountPtr = (SQLSMALLINT)stmt->stmt->n_param;
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLGetData(SQLHSTMT StatementHandle, SQLUSMALLINT Col_or_Param_Num, SQLSMALLINT TargetType,
                     SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr) {

	return WithStatementResult(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!TargetValuePtr) {
			return SQL_ERROR;
		}

		if (!stmt->chunk) {
			return SQL_ERROR;
		}
		auto val = stmt->chunk->GetValue(Col_or_Param_Num - 1, stmt->chunk_row);
		if (val.is_null) {
			if (!StrLen_or_IndPtr) {
				return SQL_ERROR;
			}
			*StrLen_or_IndPtr = SQL_NULL_DATA;
			return SQL_SUCCESS;
		}

		switch (TargetType) {
		case SQL_C_SLONG:
			assert(BufferLength >= sizeof(int));
			Store<int>(val.GetValue<int>(), (data_ptr_t)TargetValuePtr);
			return SQL_SUCCESS;

		case SQL_CHAR: {
			auto out_len = snprintf((char *)TargetValuePtr, BufferLength, "%s", val.GetValue<string>().c_str());

			if (StrLen_or_IndPtr) {
				*StrLen_or_IndPtr = out_len;
			}
			return SQL_SUCCESS;
		}
			// TODO other types
		default:
			return SQL_ERROR;
		}
	});
}

SQLRETURN SQLBindCol(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                     SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr) {
	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		auto col_nr_internal = ColumnNumber - 1;
		if (col_nr_internal >= stmt->bound_cols.size()) {
			stmt->bound_cols.resize(col_nr_internal);
		}

		stmt->bound_cols[col_nr_internal].type = TargetType;
		stmt->bound_cols[col_nr_internal].ptr = TargetValuePtr;
		stmt->bound_cols[col_nr_internal].len = BufferLength;
		stmt->bound_cols[col_nr_internal].strlen_or_ind = StrLen_or_IndPtr;

		return SQL_SUCCESS;
	});
}

SQLRETURN SQLFetch(SQLHSTMT StatementHandle) {
	return WithStatementResult(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!stmt->open) {
			return SQL_NO_DATA;
		}
		if (!stmt->chunk || stmt->chunk_row >= stmt->chunk->size() - 1) {
			// TODO try /catch
			try {
				stmt->chunk = stmt->res->Fetch();
			} catch (Exception &e) {
				// TODO this is quite dirty, we should have separate error holder
				stmt->res->error = e.what();
				stmt->res->success = false;
				stmt->open = false;
				return SQL_ERROR;
			}
			if (!stmt->chunk) {
				stmt->open = false;
				return SQL_NO_DATA;
			}
			stmt->chunk_row = -1;
		}
		if (stmt->rows_fetched_ptr) {
			(*stmt->rows_fetched_ptr)++;
		}
		stmt->chunk_row++;

		// now fill buffers in fetch if set
		// TODO actually vectorize this
		for (idx_t col_idx = 0; col_idx < stmt->stmt->ColumnCount(); col_idx++) {
			auto bound_buf = stmt->bound_cols[col_idx];
			if (bound_buf.IsBound()) {
				if (!SQL_SUCCEEDED(SQLGetData(StatementHandle, col_idx + 1, bound_buf.type, bound_buf.ptr,
				                              bound_buf.len, bound_buf.strlen_or_ind))) {
					return SQL_ERROR;
				}
			}
		}

		assert(stmt->chunk);
		assert(stmt->chunk_row < stmt->chunk->size());
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLFetchScroll(SQLHSTMT StatementHandle, SQLSMALLINT FetchOrientation, SQLLEN FetchOffset) {

	if (FetchOrientation != SQL_FETCH_NEXT) {
		return SQL_ERROR;
	}
	return SQLFetch(StatementHandle);
}

SQLRETURN SQLDescribeCol(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
                         SQLSMALLINT BufferLength, SQLSMALLINT *NameLengthPtr, SQLSMALLINT *DataTypePtr,
                         SQLULEN *ColumnSizePtr, SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr) {

	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (ColumnNumber > stmt->stmt->ColumnCount()) {
			return SQL_ERROR;
		}
		if (ColumnName && BufferLength > 0) {
			auto out_len =
			    snprintf((char *)ColumnName, BufferLength, "%s", stmt->stmt->GetNames()[ColumnNumber - 1].c_str());
			if (NameLengthPtr) {
				*NameLengthPtr = out_len;
			}
		}
		if (DataTypePtr) {
			*DataTypePtr = SQL_UNKNOWN_TYPE; // TODO
		}
		if (ColumnSizePtr) {
			*ColumnSizePtr = 0;
		}
		if (DecimalDigitsPtr) {
			*DecimalDigitsPtr = 0;
		}
		if (NullablePtr) {
			*NullablePtr = SQL_NULLABLE_UNKNOWN;
		}
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLColAttribute(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLUSMALLINT FieldIdentifier,
                          SQLPOINTER CharacterAttributePtr, SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr,
                          SQLLEN *NumericAttributePtr) {
	throw std::runtime_error("SQLColAttribute");
}

static void OdbcStringify(string &s, SQLCHAR *out_buf, SQLSMALLINT buf_len, SQLSMALLINT *out_len) {
	auto printf_len = snprintf((char *)out_buf, buf_len, "%s", s.c_str());
	if (out_len) {
		*out_len = printf_len;
	}
}

SQLRETURN SQLGetDiagRec(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLCHAR *SQLState,
                        SQLINTEGER *NativeErrorPtr, SQLCHAR *MessageText, SQLSMALLINT BufferLength,
                        SQLSMALLINT *TextLengthPtr) {

	if (!Handle) {
		return SQL_ERROR;
	}
	if (RecNumber != 1) { // TODO is it just trying increasing rec numbers?
		return SQL_ERROR;
	}

	if (SQLState) {
		*SQLState = 0;
	}

	if (NativeErrorPtr) {
		*NativeErrorPtr = 0; // we don't have error codes
	}

	auto *hdl = (OdbcHandle *)Handle;

	switch (HandleType) {
	case SQL_HANDLE_DBC: {
		// TODO return connection errors here
		return SQL_ERROR;
	}
	case SQL_HANDLE_DESC:
		throw std::runtime_error("SQL_HANDLE_DESC");
	case SQL_HANDLE_ENV: {
		// dont think we can have errors here
		return SQL_ERROR;
	}
	case SQL_HANDLE_STMT: {
		if (hdl->type != OdbcHandleType::STMT) {
			return SQL_ERROR;
		}
		auto *stmt = (OdbcHandleStmt *)hdl;
		if (stmt->stmt && !stmt->stmt->success) {
			OdbcStringify(stmt->stmt->error, MessageText, BufferLength, TextLengthPtr);
			return SQL_SUCCESS;
		}
		if (stmt->res && !stmt->res->success) {
			OdbcStringify(stmt->res->error, MessageText, BufferLength, TextLengthPtr);
			return SQL_SUCCESS;
		}
		return SQL_ERROR;
	}
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier,
                          SQLPOINTER DiagInfoPtr, SQLSMALLINT BufferLength, SQLSMALLINT *StringLengthPtr) {
	throw std::runtime_error("SQLGetDiagField");
}

SQLRETURN SQLGetInfo(SQLHDBC ConnectionHandle, SQLUSMALLINT InfoType, SQLPOINTER InfoValuePtr, SQLSMALLINT BufferLength,
                     SQLSMALLINT *StringLengthPtr) {

	// TODO more from fun list
	// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlgetinfo-function?view=sql-server-ver15

	switch (InfoType) {
	case SQL_DRIVER_NAME:
	case SQL_DBMS_NAME: {
		string dbname = "DuckDB";
		OdbcStringify(dbname, (SQLCHAR *)InfoValuePtr, BufferLength, StringLengthPtr);
		return SQL_SUCCESS;
	}
	case SQL_DBMS_VER: {
		SQLHDBC stmt;

		if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_STMT, ConnectionHandle, &stmt))) {
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_ERROR;
		}
		if (!SQL_SUCCEEDED(SQLExecDirect(stmt, (SQLCHAR *)"SELECT library_version FROM pragma_version()", SQL_NTS))) {
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_ERROR;
		}
		if (!SQL_SUCCEEDED(SQLFetch(stmt))) {
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_ERROR;
		}
		if (!SQL_SUCCEEDED(SQLGetData(stmt, 1, SQL_C_CHAR, InfoValuePtr, BufferLength, (SQLLEN *)StringLengthPtr))) {
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
			return SQL_ERROR;
		}
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
		return SQL_SUCCESS;
	}
	case SQL_NON_NULLABLE_COLUMNS:
		// TODO assert buffer length >= sizeof(SQLUSMALLINT)
		Store<SQLUSMALLINT>(SQL_NNC_NON_NULL, (data_ptr_t)InfoValuePtr);
		return SQL_SUCCESS;

	case SQL_ODBC_INTERFACE_CONFORMANCE:
		// TODO assert buffer length >= sizeof(SQLUINTEGER)

		Store<SQLUINTEGER>(SQL_OIC_CORE, (data_ptr_t)InfoValuePtr);

		return SQL_SUCCESS;

	case SQL_CREATE_TABLE:
		// TODO assert buffer length >= sizeof(SQLUINTEGER)

		Store<SQLUINTEGER>(SQL_CT_CREATE_TABLE, (data_ptr_t)InfoValuePtr);

		return SQL_SUCCESS;

	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLRowCount(SQLHSTMT StatementHandle, SQLLEN *RowCountPtr) {
	return WithStatementResult(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!RowCountPtr) {
			return SQL_ERROR;
		}
		// TODO implement
		*RowCountPtr = -1;
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLColumns(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1, SQLCHAR *SchemaName,
                     SQLSMALLINT NameLength2, SQLCHAR *TableName, SQLSMALLINT NameLength3, SQLCHAR *ColumnName,
                     SQLSMALLINT NameLength4) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) { return SQL_ERROR; });
}

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqltables-function
SQLRETURN SQLTables(SQLHSTMT StatementHandle, SQLCHAR *CatalogName, SQLSMALLINT NameLength1, SQLCHAR *SchemaName,
                    SQLSMALLINT NameLength2, SQLCHAR *TableName, SQLSMALLINT NameLength3, SQLCHAR *TableType,
                    SQLSMALLINT NameLength4) {

	// TODO make this NTS business a function
	string catalog_name = NameLength1 == SQL_NTS ? string((const char *)CatalogName)
	                                             : string((const char *)CatalogName, (size_t)NameLength1);

	string schema_name = NameLength2 == SQL_NTS ? string((const char *)SchemaName)
	                                            : string((const char *)SchemaName, (size_t)NameLength2);

	string table_name =
	    NameLength3 == SQL_NTS ? string((const char *)TableName) : string((const char *)TableName, (size_t)NameLength3);

	string table_type = NameLength4 == SQL_NTS ? string((const char *)CatalogName)
	                                           : string((const char *)CatalogName, (size_t)NameLength4);

	// special cases
	if (catalog_name == string(SQL_ALL_CATALOGS) && NameLength2 == 0 && NameLength2 == 0) {
		if (!SQL_SUCCEEDED(SQLExecDirect(StatementHandle,
		                                 (SQLCHAR *)"SELECT '' \"TABLE_CAT\", NULL \"TABLE_SCHEM\", NULL "
		                                            "\"TABLE_NAME\", NULL \"TABLE_TYPE\" , NULL \"REMARKS\"",
		                                 SQL_NTS))) {
			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	}

	if (schema_name == string(SQL_ALL_SCHEMAS) && NameLength1 == 0 && NameLength3 == 0) {
		if (!SQL_SUCCEEDED(
		        SQLExecDirect(StatementHandle,
		                      (SQLCHAR *)"SELECT '' \"TABLE_CAT\", schema_name \"TABLE_SCHEM\", NULL \"TABLE_NAME\", "
		                                 "NULL \"TABLE_TYPE\" , NULL \"REMARKS\" FROM information_schema.schemata",
		                      SQL_NTS))) {
			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	}

	if (table_type == string(SQL_ALL_TABLE_TYPES) && NameLength1 == 0 && NameLength2 == 0 && NameLength3 == 0) {
		return SQL_ERROR; // TODO
	}

	// TODO make this a nice template? also going to use this for SQLColumns etc.

	if (!SQL_SUCCEEDED(SQLPrepare(
	        StatementHandle,
	        (SQLCHAR
	             *)"SELECT table_catalog \"TABLE_CAT\", table_schema \"TABLE_SCHEM\", table_name \"TABLE_NAME\", CASE "
	               "WHEN table_type='BASE TABLE' THEN 'TABLE' ELSE table_type END \"TABLE_TYPE\" , '' \"REMARKS\"  "
	               "FROM information_schema.tables WHERE table_schema LIKE ? AND table_name LIKE ? and table_type = ?",
	        SQL_NTS))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLBindParameter(StatementHandle, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0, 0,
	                                    SchemaName, NameLength2, nullptr))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLBindParameter(StatementHandle, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0, 0,
	                                    TableName, NameLength3, nullptr))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLBindParameter(StatementHandle, 3, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_UNKNOWN_TYPE, 0, 0,
	                                    TableType, NameLength4, nullptr))) {
		return SQL_ERROR;
	}

	if (!SQL_SUCCEEDED(SQLExecute(StatementHandle))) {
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}

SQLRETURN SQLCancel(SQLHSTMT StatementHandle) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) { return SQL_SUCCESS; });
}

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqldescribeparam-function
SQLRETURN SQLDescribeParam(SQLHSTMT StatementHandle, SQLUSMALLINT ParameterNumber, SQLSMALLINT *DataTypePtr,
                           SQLULEN *ParameterSizePtr, SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr) {
	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (ParameterNumber < 0 || ParameterNumber > stmt->stmt->n_param) {
			return SQL_ERROR;
		}
		// TODO make global maps with type mappings for duckdb <> odbc
		auto odbc_type = SQL_UNKNOWN_TYPE;
		auto odbc_size = 0;
		auto param_type_id = stmt->stmt->data->GetType(ParameterNumber).id();
		switch (param_type_id) {
		case LogicalTypeId::VARCHAR:
			odbc_type = SQL_VARCHAR;
			odbc_size = SQL_NO_TOTAL;
			break;
		case LogicalTypeId::FLOAT:
			odbc_type = SQL_FLOAT;
			odbc_size = sizeof(float);
			break;
		case LogicalTypeId::DOUBLE:
			odbc_type = SQL_DOUBLE;
			odbc_size = sizeof(double);
			break;
		case LogicalTypeId::SMALLINT:
			odbc_type = SQL_SMALLINT;
			odbc_size = sizeof(int16_t);
			break;
		case LogicalTypeId::INTEGER:
			odbc_type = SQL_INTEGER;
			odbc_size = sizeof(int32_t);
			break;
		default:
			// TODO probably more types should be supported here ay
			return SQL_ERROR;
		}
		if (DataTypePtr) {
			*DataTypePtr = odbc_type;
		}
		if (ParameterSizePtr) {
			*ParameterSizePtr = odbc_size;
		}
		// TODO DecimalDigitsPtr
		if (NullablePtr) {
			*NullablePtr = SQL_NULLABLE_UNKNOWN;
		}
		return SQL_SUCCESS;
	});
}

} // extern "C"