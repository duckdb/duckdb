#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

using namespace duckdb;

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
		case SQL_C_CHAR:
			res = Value(OdbcUtils::ReadString(ParameterValuePtr, BufferLength));
			break;
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
	return ExecuteStmt(StatementHandle);
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

SQLRETURN SQLBindCol(SQLHSTMT StatementHandle, SQLUSMALLINT ColumnNumber, SQLSMALLINT TargetType,
                     SQLPOINTER TargetValuePtr, SQLLEN BufferLength, SQLLEN *StrLen_or_IndPtr) {
	return WithStatementPrepared(StatementHandle, [&](OdbcHandleStmt *stmt) {
		size_t col_nr_internal = ColumnNumber - 1;
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
