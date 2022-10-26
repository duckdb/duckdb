#include "odbc_diagnostic.hpp"

using duckdb::DiagHeader;
using duckdb::DiagRecord;
using duckdb::OdbcDiagnostic;
using duckdb::SQLState;
using duckdb::SQLStateType;
using std::string;

// OdbcDiagnostic static initializations and functions ******************************
const std::unordered_map<SQLINTEGER, std::string> OdbcDiagnostic::MAP_DYNAMIC_FUNCTION = {
    {SQL_DIAG_ALTER_DOMAIN, "ALTER DOMAIN"},
    {SQL_DIAG_ALTER_TABLE, "ALTER TABLE"},
    {SQL_DIAG_CREATE_ASSERTION, "CREATE ASSERTION"},
    {SQL_DIAG_CREATE_CHARACTER_SET, "CREATE CHARACTER SET"},
    {SQL_DIAG_CREATE_COLLATION, "CREATE COLLATION"},
    {SQL_DIAG_CREATE_DOMAIN, "CREATE DOMAIN"},
    {SQL_DIAG_CREATE_INDEX, "CREATE INDEX"},
    {SQL_DIAG_CREATE_TABLE, "CREATE TABLE"},
    {SQL_DIAG_CREATE_VIEW, "CREATE VIEW"},
    {SQL_DIAG_SELECT_CURSOR, "SELECT CURSOR"},
    {SQL_DIAG_DYNAMIC_DELETE_CURSOR, "DYNAMIC DELETE CURSOR"},
    {SQL_DIAG_DELETE_WHERE, "DELETE WHERE"},
    {SQL_DIAG_DROP_ASSERTION, "DROP ASSERTION"},
    {SQL_DIAG_DROP_CHARACTER_SET, "DROP CHARACTER SET"},
    {SQL_DIAG_DROP_COLLATION, "DROP COLLATION"},
    {SQL_DIAG_DROP_DOMAIN, "DROP DOMAIN"},
    {SQL_DIAG_DROP_INDEX, "DROP INDEX"},
    {SQL_DIAG_DROP_SCHEMA, "DROP SCHEMA"},
    {SQL_DIAG_DROP_TABLE, "DROP TABLE"},
    {SQL_DIAG_DROP_TRANSLATION, "DROP TRANSLATION"},
    {SQL_DIAG_DROP_VIEW, "DROP VIEW"},
    {SQL_DIAG_GRANT, "GRANT"},
    {SQL_DIAG_INSERT, "INSERT"},
    {SQL_DIAG_CALL, "CALL"},
    {SQL_DIAG_REVOKE, "REVOKE"},
    {SQL_DIAG_CREATE_SCHEMA, "CREATE SCHEMA"},
    {SQL_DIAG_CREATE_TRANSLATION, "CREATE TRANSLATION"},
    {SQL_DIAG_DYNAMIC_UPDATE_CURSOR, "DYNAMIC UPDATE CURSOR"},
    {SQL_DIAG_UPDATE_WHERE, "UPDATE WHERE"},
    {SQL_DIAG_UNKNOWN_STATEMENT, ""}};

const std::set<std::string> OdbcDiagnostic::SET_ODBC3_SUBCLASS_ORIGIN = {
    {"01S00"}, {"01S01"}, {"01S02"}, {"01S06"}, {"01S07"}, {"07S01"}, {"08S01"}, {"21S01"}, {"21S02"},
    {"25S01"}, {"25S02"}, {"25S03"}, {"42S01"}, {"42S02"}, {"42S11"}, {"42S12"}, {"42S21"}, {"42S22"},
    {"HY095"}, {"HY097"}, {"HY098"}, {"HY099"}, {"HY100"}, {"HY101"}, {"HY105"}, {"HY107"}, {"HY109"},
    {"HY110"}, {"HY111"}, {"HYT00"}, {"HYT01"}, {"IM001"}, {"IM002"}, {"IM003"}, {"IM004"}, {"IM005"},
    {"IM006"}, {"IM007"}, {"IM008"}, {"IM010"}, {"IM011"}, {"IM012"}};

const std::unordered_map<SQLStateType, SQLState, duckdb::EnumClassHash> OdbcDiagnostic::MAP_ODBC_SQL_STATES = {
    {SQLStateType::GENERAL_WARNING, {"01000", "General warning"}},
    {SQLStateType::CURSOR_CONFLICT, {"01001", "Cursor operation conflict"}},
    {SQLStateType::DISCONNECT_ERROR, {"01002", "Disconnect error"}},
    {SQLStateType::NULL_VALUE_ELIM, {"01003", "NULL value eliminated in set function"}},
    {SQLStateType::STR_RIGHT_TRUNCATE, {"01004", "String data, right truncated"}},
    {SQLStateType::PRIVILEGE_NOT_REVOKED, {"01006", "Privilege not revoked"}},
    {SQLStateType::PRIVILEGE_NOT_GRANTED, {"01007", "Privilege not granted"}},
    {SQLStateType::INVALID_CONNECTION_STR_ATTR, {"01S00", "Invalid connection string attribute"}},
    {SQLStateType::ERROR_ROW, {"01S01", "Error in row"}},
    {SQLStateType::OPTION_VALUE_CHANGED, {"01S02", "Option value changed"}},
    {SQLStateType::FETCH_BEFORE_FIRST_RESULT_SET,
     {"01S06", "Attempt to fetch before the result set returned the first rowset"}},
    {SQLStateType::FRACTIONAL_TRUNCATE, {"01S07", "Fractional truncation"}},
    {SQLStateType::ERROR_SAVE_DSN_FILE, {"01S08", "Error saving file DSN"}},
    {SQLStateType::INVALID_KEYWORD, {"01S09", "Invalid keyword"}},
    {SQLStateType::COUNT_FIELD_INCORRECT, {"07002", "COUNT field incorrect"}},
    {SQLStateType::PREPARE_STMT_NO_CURSOR, {"07005", "Prepared statement not a cursor-specification"}},
    {SQLStateType::RESTRICTED_DATA_TYPE, {"07006", "Restricted data type attribute violation"}},
    {SQLStateType::RETRICT_PARAMETER_VALUE, {"07007", "Restricted parameter value violation"}},
    {SQLStateType::INVALID_DESC_INDEX, {"07009", "Invalid descriptor index"}},
    {SQLStateType::INVALID_USE_DEFAULT_PARAMETER, {"07S01", "Invalid use of default parameter"}},
    {SQLStateType::CLIENT_UNABLE_TO_CONNECT, {"08001", "Client unable to establish connection"}},
    {SQLStateType::CONNECTION_NAME_IN_USE, {"08002", "Connection name in use"}},
    {SQLStateType::CONNECTION_NOT_OPEN, {"08003", "Connection not open"}},
    {SQLStateType::SERVER_REJECT_CONNECTION, {"08004", "Server rejected the connection"}},
    {SQLStateType::CONNECTION_FAIL_TRANSACTION, {"08007", "Connection failure during transaction"}},
    {SQLStateType::LINK_FAILURE, {"08S01", "Communication link failure"}},
    {SQLStateType::FEATURE_NOT_SUPPORTED, {"0A000", "Feature not supported"}},
    {SQLStateType::LIST_INSERT_MATCH_ERROR, {"21S01", "Insert value list does not match column list"}},
    {SQLStateType::DERIVED_TABLE_MATCH_ERROR, {"21S02", "Degree of derived table does not match column list"}},
    {SQLStateType::STR_RIGHT_TRUNCATE2, {"22001", "String data, right truncated"}},
    {SQLStateType::INDICATOR_VARIABLE_NOT_SUPPLIED, {"22002", "Indicator variable required but not supplied"}},
    {SQLStateType::NUMERIC_OUT_RANGE, {"22003", "Numeric value out of range"}},
    {SQLStateType::INVALID_DATATIME_FORMAT, {"22007", "Invalid datetime format"}},
    {SQLStateType::DATATIME_OVERVLOW, {"22008", "Datetime field overflow"}},
    {SQLStateType::DIVISION_BY_ZERO, {"22012", "Division by zero"}},
    {SQLStateType::INTERVAL_OVERFLOW, {"22015", "Interval field overflow"}},
    {SQLStateType::INVALID_CAST_CHAR, {"22018", "Invalid character value for cast specification"}},
    {SQLStateType::INVALID_ESCAPE_CHAR, {"22019", "Invalid escape character"}},
    {SQLStateType::INVALID_ESCAPE_SEQ, {"22025", "Invalid escape sequence"}},
    {SQLStateType::STR_LEN_MISMATCH, {"22026", "String data, length mismatch"}},
    {SQLStateType::CONSTRAINT_VIOLATION, {"23000", "Integrity constraint violation"}},
    {SQLStateType::INVALID_CURSOR_STATE, {"24000", "Invalid cursor state"}},
    {SQLStateType::INVALID_TRANSACTION_STATE, {"25000", "Invalid transaction state"}},
    {SQLStateType::TRANSACTION_STATE_UNKNOWN, {"25S01", "Transaction state unknown"}},
    {SQLStateType::TRANSACTION_STILL_ACTIVE, {"25S02", "Transaction is still active"}},
    {SQLStateType::TRANSACTION_ROLLED_BACK, {"25S03", "Transaction is rolled back"}},
    {SQLStateType::INVALID_AUTH, {"28000", "Invalid authorization specification"}},
    {SQLStateType::INVALID_CURSOR_NAME, {"34000", "Invalid cursor name"}},
    {SQLStateType::DUPLICATE_CURSOR_NAME, {"3C000", "Duplicate cursor name"}},
    {SQLStateType::INVALID_CATALOG_NAME, {"3D000", "Invalid catalog name"}},
    {SQLStateType::INVALID_SCHEMA_NAME, {"3F000", "Invalid schema name"}},
    {SQLStateType::SERIALIZATION_FAILURE, {"40001", "Serialization failure"}},
    {SQLStateType::CONSTRAINT_VIOLATION2, {"40002", "Integrity constraint violation"}},
    {SQLStateType::STMT_COMPLETION_UNKNOWN, {"40003", "Statement completion unknown"}},
    {SQLStateType::SYNTAX_ERROR_OR_ACCESS_VIOLATION, {"42000", "Syntax error or access violation"}},
    {SQLStateType::TABLE_OR_VIEW_ALREADY_EXIST, {"42S01", "Base table or view already exists"}},
    {SQLStateType::TABLE_OR_VIEW_NOT_FOUND, {"42S02", "Base table or view not found"}},
    {SQLStateType::INDEX_ALREADY_EXIST, {"42S11", "Index already exists"}},
    {SQLStateType::INDEX_NOT_FOUND, {"42S12", "Index not found"}},
    {SQLStateType::COLUMN_ALREADY_EXIST, {"42S21", "Column already exists"}},
    {SQLStateType::COLUMN_NOT_FOUND, {"42S22", "Column not found"}},
    {SQLStateType::WITH_CHECK_POINT_VIOLARION, {"44000", "WITH CHECK OPTION violation"}},
    {SQLStateType::GENERAL_ERROR, {"HY000", "General error"}},
    {SQLStateType::MEMORY_ALLOCATION_ERROR, {"HY001", "Memory allocation error"}},
    {SQLStateType::INVALID_APP_BUFFER_TYPE, {"HY003", "Invalid application buffer type"}},
    {SQLStateType::INVALD_SQL_TYPE, {"HY004", "Invalid SQL data type"}},
    {SQLStateType::STMT_NOT_PREPARED, {"HY007", "Associated statement is not prepared"}},
    {SQLStateType::OPERATION_CANCELLED, {"HY008", "Operation canceled"}},
    {SQLStateType::INVALIDE_ARGUMENT, {"HY009", "Invalid argument value"}},
    {SQLStateType::FUNCTION_SEQ_ERROR, {"HY010", "Function sequence error"}},
    {SQLStateType::ATTR_CANNOT_BE_SET_NOW, {"HY011", "Attribute cannot be set now"}},
    {SQLStateType::INVALID_TRANSACTION_OP_CODE, {"HY012", "Invalid transaction operation code"}},
    {SQLStateType::MEMORY_MANAGEMENT_ERROR, {"HY013", "Memory management error"}},
    {SQLStateType::NUMBER_HANDLES_EXCEEDED, {"HY014", "Limit on the number of handles exceeded"}},
    {SQLStateType::NO_CURSOR_NAME_AVAILABLE, {"HY015", "No cursor name available"}},
    {SQLStateType::CANNOT_MODIFY_IRD, {"HY016", "Cannot modify an implementation row descriptor"}},
    {SQLStateType::INVALID_USE_AUTO_ALLOC_DESCRIPTOR,
     {"HY017", "Invalid use of an automatically allocated descriptor handle"}},
    {SQLStateType::SERVER_DECLINED_CANCEL_REQUEST, {"HY018", "Server declined cancel request"}},
    {SQLStateType::NON_CHAR_BIN_SENT_IN_PIECES, {"HY019", "Non-character and non-binary data sent in pieces"}},
    {SQLStateType::ATTEMPT_CONCAT_NULL_VALUE, {"HY020", "Attempt to concatenate a null value"}},
    {SQLStateType::INCONSISTENT_DESC_INFO, {"HY021", "Inconsistent descriptor information"}},
    {SQLStateType::INVALID_ATTR_VALUE, {"HY024", "Invalid attribute value"}},
    {SQLStateType::INVALID_STR_BUFF_LENGTH, {"HY090", "Invalid string or buffer length"}},
    {SQLStateType::INVALD_DESC_FIELD_ID, {"HY091", "Invalid descriptor field identifier"}},
    {SQLStateType::INVALID_ATTR_OPTION_ID, {"HY092", "Invalid attribute/option identifier"}},
    {SQLStateType::FUNCTION_TYPE_OUT_RANGE, {"HY095", "Function type out of range"}},
    {SQLStateType::INFO_TYPE_OUT_RANGE, {"HY096", "Information type out of range"}},
    {SQLStateType::COLUMN_TYPE_OUT_RANGE, {"HY097", "Column type out of range"}},
    {SQLStateType::SCOPE_TYPE_OUT_RANGE, {"HY098", "Scope type out of range"}},
    {SQLStateType::NULL_TYPE_OUT_RANGE, {"HY099", "Nullable type out of range"}},
    {SQLStateType::UNIQ_OPTION_TYPE_OUT_RANGE, {"HY100", "Uniqueness option type out of range"}},
    {SQLStateType::ACCURARY_OPTION_TYPE_OUT_RANGE, {"HY101", "Accuracy option type out of range"}},
    {SQLStateType::INVALID_RETRIEVAL_CODE, {"HY103", "Invalid retrieval code"}},
    {SQLStateType::INVALID_PREC_SCALE_TYPE, {"HY104", "Invalid precision or scale value"}},
    {SQLStateType::INVALID_PARAMETER_TYPE, {"HY105", "Invalid parameter type"}},
    {SQLStateType::FETCH_TYPE_OUT_RANGE, {"HY106", "Fetch type out of range"}},
    {SQLStateType::ROW_VALUE_OUT_RANGE, {"HY107", "Row value out of range"}},
    {SQLStateType::INVALID_CURSOR_POS, {"HY109", "Invalid cursor position"}},
    {SQLStateType::INVALID_DRIVER_COMPLETION, {"HY110", "Invalid driver completion"}},
    {SQLStateType::INVALID_BOOKMARK_VALUE, {"HY111", "Invalid bookmark value"}},
    {SQLStateType::NOT_SUPPORT_ASYNC_FUNCT_EXECUTION,
     {"HY114", "Driver does not support connection-level asynchronous function execution"}},
    {SQLStateType::SQLENDTRAN_ASYNC_FUNCT_EXECUTION,
     {"HY115", "SQLEndTran is not allowed for an environment that contains a connection with asynchronous function "
               "execution enabled"}},
    {SQLStateType::CONNECTION_UNKNOW_TRANSACTION_STATE,
     {"HY117", "Connection is suspended due to unknown transaction state. Only disconnect and read-only functions are "
               "allowed."}},
    {SQLStateType::CURSOR_DRIVER_POOLING_SAME_TIME,
     {"HY121", "Cursor Library and Driver-Aware Pooling cannot be enabled at the same time"}},
    {SQLStateType::OPT_FEATURE_NOT_IMPLEMENTED, {"HYC00", "Optional feature not implemented"}},
    {SQLStateType::TIMEOUT_EXPIRED, {"HYT00", "Timeout expired"}},
    {SQLStateType::CONNECTION_TIMEOUT_EXPIRED, {"HYT01", "Connection timeout expired"}},
    {SQLStateType::DRIVER_NOT_SUPPORT_FUNCTION, {"IM001", "Driver does not support this function"}},
    {SQLStateType::DSN_NOT_FOUND_NO_DEFAULT, {"IM002", "Data source not found and no default driver specified"}},
    {SQLStateType::DRIVER_COULD_NOT_BE_CONNECTED, {"IM003", "Specified driver could not be connected to"}},
    {SQLStateType::ALLOC_HANDLE_ENV_FAIL, {"IM004", "Driver's SQLAllocHandle on SQL_HANDLE_ENV failed"}},
    {SQLStateType::ALLOC_HANDLE_DBC_FAIL, {"IM005", "Driver's SQLAllocHandle on SQL_HANDLE_DBC failed"}},
    {SQLStateType::SET_CONNECTION_ATTR_FAIL, {"IM006", "Driver's SQLSetConnectAttr failed"}},
    {SQLStateType::NO_DSN_OR_DRIVER, {"IM007", "No data source or driver specified; dialog prohibited"}},
    {SQLStateType::DIALOG_FAIL, {"IM008", "Dialog failed"}},
    {SQLStateType::UNABLE_CONNECT_TO_TRANSLATION_DLL, {"IM009", "Unable to connect to translation DLL"}},
    {SQLStateType::DSN_TOO_LONG, {"IM010", "Data source name too long"}},
    {SQLStateType::DRIVER_NAME_TOO_LONG, {"IM011", "Driver name too long"}},
    {SQLStateType::KEYWORD_SYNTAX_ERROR, {"IM012", "DRIVER keyword syntax error"}},
    {SQLStateType::DNS_ARCHITECTURE_MISMATCH,
     {"IM014", "The specified DSN contains an architecture mismatch between the Driver and Application"}},
    {SQLStateType::CONNECT_HANDLE_DBC_INFO_FAIL, {"IM015", "Driver's SQLConnect on SQL_HANDLE_DBC_INFO_HANDLE failed"}},
    {SQLStateType::POLLING_DISABLED_ACYNC_NOTIGICATION_MODE,
     {"IM017", "Polling is disabled in asynchronous notification mode"}},
    {SQLStateType::SHOULD_CALL_COMPLETE_ASYNC,
     {"IM018", "SQLCompleteAsync has not been called to complete the previous asynchronous operation on this handle."}},
    {SQLStateType::NOT_SUPPORT_ASYNC_NOTIFICATION, {"S1118", "Driver does not support asynchronous notification"}}};

bool OdbcDiagnostic::IsDiagRecordField(SQLSMALLINT rec_field) {
	switch (rec_field) {
	case SQL_DIAG_CLASS_ORIGIN:
	case SQL_DIAG_COLUMN_NUMBER:
	case SQL_DIAG_CONNECTION_NAME:
	case SQL_DIAG_MESSAGE_TEXT:
	case SQL_DIAG_NATIVE:
	case SQL_DIAG_ROW_NUMBER:
	case SQL_DIAG_SERVER_NAME:
	case SQL_DIAG_SQLSTATE:
	case SQL_DIAG_SUBCLASS_ORIGIN:
		return true;
	default:
		return false;
	}
}

void OdbcDiagnostic::FormatDiagnosticMessage(DiagRecord &diag_record, const std::string &data_source,
                                             const std::string &component) {
	// https://docs.microsoft.com/en-us/sql/odbc/reference/develop-app/diagnostic-messages?view=sql-server-ver15
	// [ vendor-identifier ][ ODBC-component-identifier ][ data-source-identifier ] data-source-supplied-text
	auto error_msg = diag_record.GetOriginalMessage();

	string msg = "ODBC_DuckDB";
	if (!data_source.empty()) {
		msg += "->" + data_source;
	}
	if (!component.empty()) {
		msg += "->" + component;
	}
	msg += "\n" + error_msg;

	diag_record.SetMessage(msg);
}

void OdbcDiagnostic::AddDiagRecord(DiagRecord &diag_record) {
	auto rec_idx = (SQLSMALLINT)diag_records.size();
	diag_records.emplace_back(diag_record);
	vec_record_idx.emplace_back(rec_idx);
}

void OdbcDiagnostic::AddNewRecIdx(SQLSMALLINT rec_idx) {
	auto origin_idx = vec_record_idx[rec_idx];
	auto begin = vec_record_idx.begin();
	vec_record_idx.emplace(std::next(begin, rec_idx + 1), origin_idx);
}

duckdb::idx_t OdbcDiagnostic::GetTotalRecords() {
	return vec_record_idx.size();
}

void OdbcDiagnostic::Clean() {
	header = DiagHeader();
	diag_records.clear();
	vec_record_idx.clear();
}

string OdbcDiagnostic::GetDiagDynamicFunction() {
	auto entry = MAP_DYNAMIC_FUNCTION.find(header.sql_diag_dynamic_function_code);
	if (entry == MAP_DYNAMIC_FUNCTION.end()) {
		return "";
	}
	return entry->second;
}

bool OdbcDiagnostic::VerifyRecordIndex(SQLINTEGER rec_idx) {
	return (rec_idx < (SQLINTEGER)diag_records.size() && rec_idx >= 0);
}

DiagRecord &OdbcDiagnostic::GetDiagRecord(SQLINTEGER rec_idx) {
	D_ASSERT(rec_idx < (SQLINTEGER)vec_record_idx.size() && rec_idx >= 0);
	auto origin_idx = vec_record_idx[rec_idx];
	auto diag_record = &diag_records[origin_idx];
	// getting first record, clear up vec_record_idx
	if (rec_idx == 0) {
		vec_record_idx.clear();
		for (duckdb::idx_t i = 0; i < diag_records.size(); ++i) {
			vec_record_idx.emplace_back(i);
			diag_records[i].ClearStackMsgOffset();
		}
	}
	return *diag_record;
}

std::string OdbcDiagnostic::GetDiagClassOrigin(SQLINTEGER rec_idx) {
	D_ASSERT(rec_idx < (SQLINTEGER)diag_records.size() && rec_idx >= 0);
	auto sqlstate_str = diag_records[rec_idx].sql_diag_sqlstate;
	if (sqlstate_str.find("IM") != std::string::npos) {
		return "ODBC 3.0";
	} else {
		return "ISO 9075";
	}
}

std::string OdbcDiagnostic::GetDiagSubclassOrigin(SQLINTEGER rec_idx) {
	D_ASSERT(rec_idx < (SQLINTEGER)diag_records.size() && rec_idx >= 0);
	auto sqlstate_str = diag_records[rec_idx].sql_diag_sqlstate;
	;
	if (SET_ODBC3_SUBCLASS_ORIGIN.find(sqlstate_str) != SET_ODBC3_SUBCLASS_ORIGIN.end()) {
		return "ODBC 3.0";
	} else {
		return "ISO 9075";
	}
}

// DiagRecord functions ****************************************************************
DiagRecord::DiagRecord(const std::string &msg, const SQLStateType &sqlstate_type, const std::string &server_name,
                       SQLINTEGER col_number, SQLINTEGER sql_native, SQLLEN row_number) {
	D_ASSERT(!msg.empty());

	sql_diag_message_text = msg;

	auto entry = OdbcDiagnostic::MAP_ODBC_SQL_STATES.find(sqlstate_type);
	if (entry != OdbcDiagnostic::MAP_ODBC_SQL_STATES.end()) {
		sql_diag_sqlstate = entry->second.code;
	}

	sql_diag_server_name = server_name;
	sql_diag_column_number = col_number;
	sql_diag_native = sql_native;
	sql_diag_row_number = row_number;

	stack_msg_offset.push(0);
}

DiagRecord::DiagRecord(const DiagRecord &other) {
	// calling copy assigment operator
	*this = other;
}

DiagRecord &DiagRecord::operator=(const DiagRecord &other) {
	if (&other != this) {
		sql_diag_message_text = other.sql_diag_message_text;
		sql_diag_sqlstate = other.sql_diag_sqlstate;
		sql_diag_server_name = other.sql_diag_server_name;
		sql_diag_column_number = other.sql_diag_column_number;
		sql_diag_native = other.sql_diag_native;
		sql_diag_row_number = other.sql_diag_row_number;
		stack_msg_offset = other.stack_msg_offset;
	}
	return *this;
}

std::string DiagRecord::GetOriginalMessage() {
	return sql_diag_message_text;
}

std::string DiagRecord::GetMessage(SQLSMALLINT buff_length) {
	duckdb::idx_t last_offset = stack_msg_offset.top();
	auto new_offset = last_offset + buff_length;
	if (new_offset >= sql_diag_message_text.size()) {
		ClearStackMsgOffset();
	} else {
		stack_msg_offset.push(new_offset);
	}
	return sql_diag_message_text.substr(last_offset);
}

void DiagRecord::SetMessage(const std::string &new_msg) {
	D_ASSERT(!new_msg.empty());
	sql_diag_message_text = new_msg;
	ClearStackMsgOffset();
}

void DiagRecord::ClearStackMsgOffset() {
	while (!stack_msg_offset.empty() && stack_msg_offset.top() != 0) {
		stack_msg_offset.pop();
	}
}
