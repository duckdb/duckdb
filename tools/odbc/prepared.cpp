#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "api_info.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/exception.hpp"

using duckdb::Decimal;
using duckdb::hugeint_t;
using duckdb::idx_t;
using duckdb::Load;
using duckdb::LogicalType;
using duckdb::string_t;
using duckdb::Value;

bool IsNumeric(SQLSMALLINT value_type) {
	switch (value_type) {
	case SQL_C_TINYINT:
	case SQL_C_STINYINT:
	case SQL_C_UTINYINT:
	case SQL_C_SHORT:
	case SQL_C_SSHORT:
	case SQL_C_USHORT:
	case SQL_C_SLONG:
	case SQL_C_LONG:
	case SQL_C_ULONG:
	case SQL_C_SBIGINT:
	case SQL_C_UBIGINT:
	case SQL_C_FLOAT:
	case SQL_C_DOUBLE:
	case SQL_NUMERIC:
		return true;
	default:
		return false;
	}
}

SQLRETURN ValidateNumeric(int precision, int scale) {
	if (precision < 1 || precision > Decimal::MAX_WIDTH_DECIMAL || scale < 0 || scale > Decimal::MAX_WIDTH_DECIMAL ||
	    scale > precision) {
		// TODO we should use SQLGetDiagField to register the error message
		std::string msg = "Numeric precision: " + std::to_string(precision) + " and scale: " + std::to_string(scale) +
		                  " not supported.";
		return SQL_ERROR;
	}
	return SQL_SUCCESS;
}

SQLRETURN SQLBindParameter(SQLHSTMT statement_handle, SQLUSMALLINT parameter_number, SQLSMALLINT input_output_type,
                           SQLSMALLINT value_type, SQLSMALLINT parameter_type, SQLULEN column_size,
                           SQLSMALLINT decimal_digits, SQLPOINTER parameter_value_ptr, SQLLEN buffer_length,
                           SQLLEN *str_len_or_ind_ptr) {
	return duckdb::WithStatementPrepared(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (parameter_number > stmt->stmt->n_param) {
			return SQL_ERROR;
		}
		if (input_output_type != SQL_PARAM_INPUT) {
			return SQL_ERROR;
		}

		// it would appear that the parameter_type does not matter that much
		// we cast it anyway and if the cast fails we will hear about it during execution
		duckdb::Value res;
		duckdb::const_data_ptr_t dataptr;
		// cast properly to numeric type
		if (IsNumeric(value_type)) {
			auto numeric = (SQL_NUMERIC_STRUCT *)parameter_value_ptr;
			dataptr = numeric->val;
		} else {
			dataptr = (duckdb::const_data_ptr_t)parameter_value_ptr;
		}

		switch (value_type) {
		case SQL_C_CHAR:
			res = Value(duckdb::OdbcUtils::ReadString(parameter_value_ptr, buffer_length));
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
		case SQL_NUMERIC: {
			auto precision = column_size;
			if (ValidateNumeric(precision, decimal_digits) == SQL_ERROR) {
				return SQL_ERROR;
			}
			if (column_size <= Decimal::MAX_WIDTH_INT64) {
				res = Value::DECIMAL(Load<int64_t>(dataptr), precision, decimal_digits);
			} else {
				hugeint_t dec_value;
				memcpy(&dec_value.lower, dataptr, sizeof(dec_value.lower));
				memcpy(&dec_value.upper, dataptr + sizeof(dec_value.lower), sizeof(dec_value.upper));
				res = Value::DECIMAL(dec_value, precision, decimal_digits);
			}
			break;
		}
		// TODO more types
		default:
			// TODO error message?
			return SQL_ERROR;
		}
		stmt->params[parameter_number - 1] = res;
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLExecute(SQLHSTMT statement_handle) {
	return duckdb::ExecuteStmt(statement_handle);
}

SQLRETURN SQLNumResultCols(SQLHSTMT statement_handle, SQLSMALLINT *column_count_ptr) {
	return duckdb::WithStatementPrepared(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (!column_count_ptr) {
			return SQL_ERROR;
		}
		*column_count_ptr = (SQLSMALLINT)stmt->stmt->GetTypes().size();
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLNumParams(SQLHSTMT statement_handle, SQLSMALLINT *parameter_count_ptr) {
	return duckdb::WithStatementPrepared(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (!parameter_count_ptr) {
			return SQL_ERROR;
		}
		*parameter_count_ptr = (SQLSMALLINT)stmt->stmt->n_param;
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLBindCol(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLSMALLINT target_type,
                     SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {
	return duckdb::WithStatementPrepared(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		size_t col_nr_internal = column_number - 1;
		if (col_nr_internal >= stmt->bound_cols.size()) {
			stmt->bound_cols.resize(col_nr_internal);
		}

		stmt->bound_cols[col_nr_internal].type = target_type;
		stmt->bound_cols[col_nr_internal].ptr = target_value_ptr;
		stmt->bound_cols[col_nr_internal].len = buffer_length;
		stmt->bound_cols[col_nr_internal].strlen_or_ind = str_len_or_ind_ptr;

		return SQL_SUCCESS;
	});
}

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqldescribeparam-function
SQLRETURN SQLDescribeParam(SQLHSTMT statement_handle, SQLUSMALLINT parameter_number, SQLSMALLINT *data_type_ptr,
                           SQLULEN *parameter_size_ptr, SQLSMALLINT *decimal_digits_ptr, SQLSMALLINT *nullable_ptr) {
	return duckdb::WithStatementPrepared(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (parameter_number < 0 || parameter_number > stmt->stmt->n_param) {
			return SQL_ERROR;
		}
		// TODO make global maps with type mappings for duckdb <> odbc
		auto odbc_type = SQL_UNKNOWN_TYPE;
		auto odbc_size = 0;
		auto param_type_id = stmt->stmt->data->GetType(parameter_number).id();
		switch (param_type_id) {
		case duckdb::LogicalTypeId::VARCHAR:
			odbc_type = SQL_VARCHAR;
			odbc_size = SQL_NO_TOTAL;
			break;
		case duckdb::LogicalTypeId::FLOAT:
			odbc_type = SQL_FLOAT;
			odbc_size = sizeof(float);
			break;
		case duckdb::LogicalTypeId::DOUBLE:
			odbc_type = SQL_DOUBLE;
			odbc_size = sizeof(double);
			break;
		case duckdb::LogicalTypeId::SMALLINT:
			odbc_type = SQL_SMALLINT;
			odbc_size = sizeof(int16_t);
			break;
		case duckdb::LogicalTypeId::INTEGER:
			odbc_type = SQL_INTEGER;
			odbc_size = sizeof(int32_t);
			break;
		default:
			// TODO probably more types should be supported here ay
			return SQL_ERROR;
		}
		if (data_type_ptr) {
			*data_type_ptr = odbc_type;
		}
		if (parameter_size_ptr) {
			*parameter_size_ptr = odbc_size;
		}
		// TODO decimal_digits_ptr
		if (nullable_ptr) {
			*nullable_ptr = SQL_NULLABLE_UNKNOWN;
		}
		return SQL_SUCCESS;
	});
}

SQLRETURN SQLDescribeCol(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLCHAR *column_name,
                         SQLSMALLINT buffer_length, SQLSMALLINT *name_length_ptr, SQLSMALLINT *data_type_ptr,
                         SQLULEN *column_size_ptr, SQLSMALLINT *decimal_digits_ptr, SQLSMALLINT *nullable_ptr) {

	return duckdb::WithStatementPrepared(statement_handle, [&](duckdb::OdbcHandleStmt *stmt) {
		if (column_number > stmt->stmt->ColumnCount()) {
			return SQL_ERROR;
		}
		if (column_name && buffer_length > 0) {
			auto out_len =
			    snprintf((char *)column_name, buffer_length, "%s", stmt->stmt->GetNames()[column_number - 1].c_str());
			if (name_length_ptr) {
				*name_length_ptr = out_len;
			}
		}

		LogicalType col_type = stmt->stmt->GetTypes()[column_number - 1];

		if (data_type_ptr) {
			*data_type_ptr = duckdb::ApiInfo::FindRelatedSQLType(col_type.id());
		}
		if (column_size_ptr) {
			*column_size_ptr = 0;
		}
		if (decimal_digits_ptr) {
			*decimal_digits_ptr = 0;
			if (col_type.id() == duckdb::LogicalTypeId::DECIMAL) {
				*decimal_digits_ptr = duckdb::DecimalType::GetScale(col_type);
			}
		}
		if (nullable_ptr) {
			*nullable_ptr = SQL_NULLABLE_UNKNOWN;
		}
		return SQL_SUCCESS;
	});
}
