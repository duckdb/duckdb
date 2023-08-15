#include "duckdb_odbc.hpp"
#include "statement_functions.hpp"
#include "handle_functions.hpp"
#include "api_info.hpp"
#include "parameter_descriptor.hpp"

#include "duckdb/main/prepared_statement_data.hpp"

using duckdb::hugeint_t;
using duckdb::idx_t;
using duckdb::Load;
using duckdb::LogicalType;
using duckdb::Value;

/**
 * @brief Binds a buffer to a parameter marker.  See the ODBC documentation for more details, as the arguments are very
 * complex. https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindparameter-function?view=sql-server-ver16
 * @param statement_handle
 * @param parameter_number Parameter number, starting at 1 and increasing for each parameter in the statement.
 * @param input_output_type Input/output type of the bound parameter.
 * @param value_type C data type of the parameter.
 * @param parameter_type SQL data type of the parameter.
 * @param column_size The size of the column or expression in characters.
 * @param decimal_digits The decimal digits of the column or expression.
 * @param parameter_value_ptr A pointer to a buffer for the parameter value.
 * @param buffer_length The length of the parameter value buffer in bytes.
 * @param str_len_or_ind_ptr A pointer to a buffer for the parameter length or indicator value.
 * @return SQL return code
 */
SQLRETURN SQL_API SQLBindParameter(SQLHSTMT statement_handle, SQLUSMALLINT parameter_number,
                                   SQLSMALLINT input_output_type, SQLSMALLINT value_type, SQLSMALLINT parameter_type,
                                   SQLULEN column_size, SQLSMALLINT decimal_digits, SQLPOINTER parameter_value_ptr,
                                   SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {
	return duckdb::BindParameterStmt(statement_handle, parameter_number, input_output_type, value_type, parameter_type,
	                                 column_size, decimal_digits, parameter_value_ptr, buffer_length,
	                                 str_len_or_ind_ptr);
}

/**
 * @brief
 * Executes a prepared statement.
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlexecute-function?view=sql-server-ver16
 * @param statement_handle The statement handle
 */
SQLRETURN SQL_API SQLExecute(SQLHSTMT statement_handle) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMT(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	return duckdb::BatchExecuteStmt(hstmt);
}

/**
 * @brief
 * Returns the number of columns in the result set.
 * @param statement_handle The statement handle
 * @param column_count_ptr The number of columns in the result set, gets set by the function
 */
SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT statement_handle, SQLSMALLINT *column_count_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMTPrepared(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	if (!column_count_ptr) {
		return SQL_ERROR;
	}
	*column_count_ptr = (SQLSMALLINT)hstmt->stmt->ColumnCount();

	if (hstmt->stmt->data->statement_type != duckdb::StatementType::SELECT_STATEMENT) {
		*column_count_ptr = 0;
	}
	return SQL_SUCCESS;
}

SQLRETURN SQL_API SQLNumParams(SQLHSTMT statement_handle, SQLSMALLINT *parameter_count_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMTPrepared(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	if (!parameter_count_ptr) {
		return SQL_ERROR;
	}
	*parameter_count_ptr = (SQLSMALLINT)hstmt->stmt->n_param;
	return SQL_SUCCESS;
}

/**
 * @brief Binds a buffer to a column in the result set.
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlbindcol-function?view=sql-server-ver16#row-wise-binding
 * @param statement_handle
 * @param column_number
 * @param target_type The C data type of the application data buffer (SQL_C_CHAR, SQL_C_LONG, etc.) (see Data Types
 * https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types?view=sql-server-ver16)
 * @param target_value_ptr A pointer to the data buffer to bind the column to, that is filled with the data from the
 * column when SQLFetch or SQLFetchScroll is called. The data type of the buffer must be the same as the data type of
 * the column
 * @param buffer_length Length of the buffer in bytes
 * @param str_len_or_ind_ptr A pointer to the length/indicator buffer to bind to the column.  Can be filled with the
 * length of the data in the column, or potentially other values depending on which function is called.
 * @return SQL return code
 */
SQLRETURN SQL_API SQLBindCol(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLSMALLINT target_type,
                             SQLPOINTER target_value_ptr, SQLLEN buffer_length, SQLLEN *str_len_or_ind_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMT(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	D_ASSERT(column_number > 0);

	if (column_number > hstmt->bound_cols.size()) {
		hstmt->bound_cols.resize(column_number);
	}

	size_t col_nr_internal = column_number - 1;

	hstmt->bound_cols[col_nr_internal].type = target_type;
	hstmt->bound_cols[col_nr_internal].ptr = target_value_ptr;
	hstmt->bound_cols[col_nr_internal].len = buffer_length;
	hstmt->bound_cols[col_nr_internal].strlen_or_ind = str_len_or_ind_ptr;

	return SQL_SUCCESS;
}

// https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqldescribeparam-function
SQLRETURN SQL_API SQLDescribeParam(SQLHSTMT statement_handle, SQLUSMALLINT parameter_number, SQLSMALLINT *data_type_ptr,
                                   SQLULEN *parameter_size_ptr, SQLSMALLINT *decimal_digits_ptr,
                                   SQLSMALLINT *nullable_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMTPrepared(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	if (parameter_number <= 0 || parameter_number > hstmt->stmt->n_param) {
		return SQL_ERROR;
	}
	// TODO make global maps with type mappings for duckdb <> odbc
	auto odbc_type = SQL_UNKNOWN_TYPE;
	auto odbc_size = 0;
	auto identifier = std::to_string(parameter_number);
	auto param_type_id = hstmt->stmt->data->GetType(identifier).id();
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
}

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT statement_handle, SQLUSMALLINT column_number, SQLCHAR *column_name,
                                 SQLSMALLINT buffer_length, SQLSMALLINT *name_length_ptr, SQLSMALLINT *data_type_ptr,
                                 SQLULEN *column_size_ptr, SQLSMALLINT *decimal_digits_ptr, SQLSMALLINT *nullable_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMTPrepared(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	if (column_number > hstmt->stmt->ColumnCount()) {
		return SQL_ERROR;
	}

	duckdb::idx_t col_idx = column_number - 1;

	duckdb::OdbcUtils::WriteString(hstmt->stmt->GetNames()[col_idx], column_name, buffer_length, name_length_ptr);

	LogicalType col_type = hstmt->stmt->GetTypes()[col_idx];

	if (data_type_ptr) {
		*data_type_ptr = duckdb::ApiInfo::FindRelatedSQLType(col_type.id());
	}
	if (column_size_ptr) {
		auto ret = duckdb::ApiInfo::GetColumnSize(hstmt->stmt->GetTypes()[col_idx], column_size_ptr);
		if (ret == SQL_ERROR) {
			*column_size_ptr = 0;
		}
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
}

/**
 * @brief Used together with SQLPutData and SQLGetData to support binding to a column or parameter data.
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlparamdata-function?view=sql-server-ver15
 * @param statement_handle
 * @param value_ptr_ptr Pointer to a buffer in which to return the address of the bound data buffer.
 * @return
 */
SQLRETURN SQL_API SQLParamData(SQLHSTMT statement_handle, SQLPOINTER *value_ptr_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMTPrepared(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	auto ret = hstmt->param_desc->GetNextParam(value_ptr_ptr);
	if (ret != SQL_NO_DATA) {
		return ret;
	}
	// should try to get value from bound columns
	return SQL_SUCCESS;
}

/**
 * @brief Allows the application to set data for a parameter or column at execution time.
 * https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/sqlputdata-function?view=sql-server-ver15
 * @param statement_handle
 * @param data_ptr Pointer to a buffer containing the data to be sent to the data source.  Must be in the C data type
 * specified in SQLBindParameter or SQLBindCol.
 * @param str_len_or_ind_ptr Length of data_ptr.
 * @return
 */
SQLRETURN SQL_API SQLPutData(SQLHSTMT statement_handle, SQLPOINTER data_ptr, SQLLEN str_len_or_ind_ptr) {
	duckdb::OdbcHandleStmt *hstmt = nullptr;
	if (ConvertHSTMTPrepared(statement_handle, hstmt) != SQL_SUCCESS) {
		return SQL_ERROR;
	}

	auto ret = hstmt->param_desc->PutData(data_ptr, str_len_or_ind_ptr);
	if (ret == SQL_SUCCESS) {
		return ret;
	}

	// should try to put value into bound columns
	return SQL_SUCCESS;
}
