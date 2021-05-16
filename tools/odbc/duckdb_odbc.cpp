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
	vector<Value> params;
	row_t chunk_row;
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
		stmt->stmt.reset();
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
	SQLFreeStmt(StatementHandle, SQL_CLOSE);
	return SQL_SUCCESS;
}

SQLRETURN SQLPrepare(SQLHSTMT StatementHandle, SQLCHAR *StatementText, SQLINTEGER TextLength) {
	return WithStatement(StatementHandle, [&](OdbcHandleStmt *stmt) {
		stmt->stmt.reset();
		stmt->res.reset();
		stmt->chunk.reset();
		stmt->params.resize(0);
		string query = TextLength == SQL_NTS ? string((const char *)StatementText)
		                                     : string((const char *)StatementText, (size_t)TextLength);
		stmt->stmt = stmt->dbc->conn->Prepare(query);
		if (!stmt->stmt->success) {
			return SQL_ERROR;
		}
		stmt->params.resize(stmt->stmt->n_param);
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
		stmt->res = stmt->stmt->Execute(stmt->params);
		if (!stmt->res->success) {
			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLNumResultCols(SQLHSTMT StatementHandle, SQLSMALLINT *ColumnCountPtr) {
	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		*ColumnCountPtr = (SQLSMALLINT)stmt->stmt->GetTypes().size();
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLGetData(SQLHSTMT StatementHandle, SQLUSMALLINT Col_or_Param_Num, SQLSMALLINT TargetType,
                     SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr) {

	return WithStatementResult(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!stmt->chunk) {
			return SQL_ERROR;
		}
		switch (TargetType) {
		case SQL_C_SLONG: {
			auto val = stmt->chunk->GetValue(Col_or_Param_Num - 1, stmt->chunk_row).GetValue<int>();
			assert(BufferLength >= sizeof(int));
			Store<int>(val, (data_ptr_t)TargetValuePtr);
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
	// TODO
	throw new std::runtime_error("SQLBindCol");
}

SQLRETURN SQLFetch(SQLHSTMT StatementHandle) {
	return WithStatementResult(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (!stmt->chunk || stmt->chunk_row >= stmt->chunk->size() - 1) {
			// TODO try /catch
			stmt->chunk = stmt->res->Fetch();
			if (!stmt->chunk) {
				return SQL_ERROR; // TODO probably need NO_DATA or so here
			}
			stmt->chunk_row = -1;
		}

		stmt->chunk_row++;
		assert(stmt->chunk);
		assert(stmt->chunk_row < stmt->chunk->size());
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLDescribeCol(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLCHAR *ColumnName,
                         SQLSMALLINT BufferLength, SQLSMALLINT *NameLengthPtr, SQLSMALLINT *DataTypePtr,
                         SQLULEN *ColumnSizePtr, SQLSMALLINT *DecimalDigitsPtr, SQLSMALLINT *NullablePtr) {

	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		if (ColumnNumber > stmt->stmt->ColumnCount()) {
			return SQL_ERROR;
		}
		if (ColumnName && NameLengthPtr && BufferLength > 0) {
			*NameLengthPtr =
			    snprintf((char *)ColumnName, BufferLength, "%s", stmt->stmt->GetNames()[ColumnNumber - 1].c_str());
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
			if (MessageText && BufferLength > 0 && TextLengthPtr) {
				*TextLengthPtr = snprintf((char *)MessageText, BufferLength, "%s", stmt->stmt->error.c_str());
			}
			return SQL_SUCCESS;
		}
		if (stmt->res && !stmt->res->success) {
			if (MessageText && BufferLength > 0 && TextLengthPtr) {
				*TextLengthPtr = snprintf((char *)MessageText, BufferLength, "%s", stmt->res->error.c_str());
			}
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
} // extern "C"