// needs to be first because BOOL
#include "duckdb.hpp"

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

struct OdbcHandleStmt : public OdbcHandle {
	OdbcHandleStmt(OdbcHandleDbc *dbc_p) : OdbcHandle(OdbcHandleType::STMT), dbc(dbc_p) {
		assert(dbc_p);
		assert(dbc_p->conn);
	};

	OdbcHandleDbc *dbc;
	unique_ptr<PreparedStatement> stmt;
	unique_ptr<QueryResult> res;
	unique_ptr<DataChunk> chunk;
	row_t chunk_row;
};
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
	return SQL_SUCCESS;
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

SQLRETURN SQLFreeStmt(SQLHSTMT StatementHandle, SQLUSMALLINT Option) {
	assert(Option == SQL_CLOSE);
	return SQLFreeHandle(SQL_HANDLE_STMT, StatementHandle);
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

SQLRETURN SQLExecDirect(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength) {
	auto prepare_status = SQLPrepare(StatementHandle, StatementText, TextLength);
	if (prepare_status != SQL_SUCCESS) {
		return SQL_ERROR;
	}
	return SQLExecute(StatementHandle);
}

SQLRETURN SQLPrepare(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength) {
	assert(StatementHandle);
	auto *hdl = (OdbcHandleStmt *)StatementHandle;
	assert(hdl->dbc);
	assert(hdl->dbc->conn);
	hdl->stmt.reset();
	hdl->res.reset();
	// TODO support explicit lengths, too
	assert(TextLength == SQL_NTS);

	auto stmt = hdl->dbc->conn->Prepare(string((const char *)StatementText));
	if (!stmt->success) {
		// TODO set error correctly (how?)
		return SQL_ERROR;
	}
	hdl->stmt = move(stmt);
	return SQL_SUCCESS;
}

SQLRETURN SQLExecute(SQLHSTMT StatementHandle) {
	assert(StatementHandle);
	auto *hdl = (OdbcHandleStmt *)StatementHandle;
	assert(hdl->stmt); // TODO make this an exception
	hdl->res.reset();
	hdl->chunk.reset();
	hdl->chunk_row = -1;
	auto result = hdl->stmt->Execute();
	if (!result->success) {
		// TODO set error correctly (how?)
		return SQL_ERROR;
	}
	hdl->res = move(result);
	return SQL_SUCCESS;
}

SQLRETURN SQLNumResultCols(SQLHSTMT StatementHandle, SQLSMALLINT *ColumnCountPtr) {
	assert(StatementHandle);
	auto *hdl = (OdbcHandleStmt *)StatementHandle;
	assert(hdl->res); // TODO make this an exception
	*ColumnCountPtr = hdl->res->types.size();
	return SQL_SUCCESS;
}

SQLRETURN SQLGetData(SQLHSTMT StatementHandle, SQLUSMALLINT Col_or_Param_Num, SQLSMALLINT TargetType,
                     SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr) {
	assert(StatementHandle);
	auto *hdl = (OdbcHandleStmt *)StatementHandle;
	assert(hdl->res); // TODO make this an exception
	assert(hdl->chunk);

	switch (TargetType) {
	case SQL_C_SLONG: {
		auto val = hdl->chunk->GetValue(Col_or_Param_Num - 1, hdl->chunk_row).GetValue<int>();
		assert(BufferLength >= sizeof(int));
		Store<int>(val, (data_ptr_t)TargetValuePtr);
		return SQL_SUCCESS;
	}
	// TODO other types
	default:
		return SQL_ERROR;
	}
}

SQLRETURN SQLBindCol(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                     SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr) {
	// TODO
	throw new std::runtime_error("SQLBindCol");
}

SQLRETURN SQLFetch(SQLHSTMT StatementHandle) {
	assert(StatementHandle);
    auto *hdl = (OdbcHandleStmt *)StatementHandle;
	assert(hdl->res); // TODO make this an exception

	if (!hdl->chunk || hdl->chunk_row >= hdl->chunk->size() - 1) {
		// TODO try /catch
		hdl->chunk = hdl->res->Fetch();
		if (!hdl->chunk) {
			return SQL_ERROR; // TODO probably need NO_DATA or so here
		}
		hdl->chunk_row = -1;
	}

	hdl->chunk_row++;
	assert(hdl->chunk);
	assert(hdl->chunk_row < hdl->chunk->size());
	return SQL_SUCCESS;
}

SQLRETURN SQLDescribeCol(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
                         SQLSMALLINT BufferLength, SQLSMALLINT *NameLengthPtr, SQLSMALLINT *DataTypePtr,
                         SQLULEN *ColumnSizePtr, SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr) {

    assert(StatementHandle);
    auto *hdl = (OdbcHandleStmt *)StatementHandle;
    assert(hdl->res); // TODO make this an exception

    assert(ColumnNumber <= hdl->res->ColumnCount());
    *NameLengthPtr = snprintf((char*) ColumnName, BufferLength, "%s", hdl->res->names[ColumnNumber - 1].c_str());
    *DataTypePtr = SQL_UNKNOWN_TYPE; // TODO

    return SQL_SUCCESS;

}
} // extern "C"