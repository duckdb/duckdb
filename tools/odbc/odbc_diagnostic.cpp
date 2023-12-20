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
    {SQLStateType::ST_01000, {"01000", "General warning"}},
    {SQLStateType::ST_01001, {"01001", "Cursor operation conflict"}},
    {SQLStateType::ST_01002, {"01002", "Disconnect error"}},
    {SQLStateType::ST_01003, {"01003", "NULL value eliminated in set function"}},
    {SQLStateType::ST_01004, {"01004", "String data, right truncated"}},
    {SQLStateType::ST_01006, {"01006", "Privilege not revoked"}},
    {SQLStateType::ST_01007, {"01007", "Privilege not granted"}},
    {SQLStateType::ST_01S00, {"01S00", "Invalid connection string attribute"}},
    {SQLStateType::ST_01S01, {"01S01", "Error in row"}},
    {SQLStateType::ST_01S02, {"01S02", "Option value changed"}},
    {SQLStateType::ST_01S06, {"01S06", "Attempt to fetch before the result set returned the first rowset"}},
    {SQLStateType::ST_01S07, {"01S07", "Fractional truncation"}},
    {SQLStateType::ST_01S08, {"01S08", "Error saving file DSN"}},
    {SQLStateType::ST_01S09, {"01S09", "Invalid keyword"}},
    {SQLStateType::ST_07002, {"07002", "COUNT field incorrect"}},
    {SQLStateType::ST_07005, {"07005", "Prepared statement not a cursor-specification"}},
    {SQLStateType::ST_07006, {"07006", "Restricted data type attribute violation"}},
    {SQLStateType::ST_07007, {"07007", "Restricted parameter value violation"}},
    {SQLStateType::ST_07009, {"07009", "Invalid descriptor index"}},
    {SQLStateType::ST_07S01, {"07S01", "Invalid use of default parameter"}},
    {SQLStateType::ST_08001, {"08001", "Client unable to establish connection"}},
    {SQLStateType::ST_08002, {"08002", "Connection name in use"}},
    {SQLStateType::ST_08003, {"08003", "Connection not open"}},
    {SQLStateType::ST_08004, {"08004", "Server rejected the connection"}},
    {SQLStateType::ST_08007, {"08007", "Connection failure during transaction"}},
    {SQLStateType::ST_08S01, {"08S01", "Communication link failure"}},
    {SQLStateType::ST_0A000, {"0A000", "Feature not supported"}},
    {SQLStateType::ST_21S01, {"21S01", "Insert value list does not match column list"}},
    {SQLStateType::ST_21S02, {"21S02", "Degree of derived table does not match column list"}},
    {SQLStateType::ST_22001, {"22001", "String data, right truncated"}},
    {SQLStateType::ST_22002, {"22002", "Indicator variable required but not supplied"}},
    {SQLStateType::ST_22003, {"22003", "Numeric value out of range"}},
    {SQLStateType::ST_22007, {"22007", "Invalid datetime format"}},
    {SQLStateType::ST_22008, {"22008", "Datetime field overflow"}},
    {SQLStateType::ST_22012, {"22012", "Division by zero"}},
    {SQLStateType::ST_22015, {"22015", "Interval field overflow"}},
    {SQLStateType::ST_22018, {"22018", "Invalid character value for cast specification"}},
    {SQLStateType::ST_22019, {"22019", "Invalid escape character"}},
    {SQLStateType::ST_22025, {"22025", "Invalid escape sequence"}},
    {SQLStateType::ST_22026, {"22026", "String data, length mismatch"}},
    {SQLStateType::ST_23000, {"23000", "Integrity constraint violation"}},
    {SQLStateType::ST_24000, {"24000", "Invalid cursor state"}},
    {SQLStateType::ST_25000, {"25000", "Invalid transaction state"}},
    {SQLStateType::ST_25S01, {"25S01", "Transaction state unknown"}},
    {SQLStateType::ST_25S02, {"25S02", "Transaction is still active"}},
    {SQLStateType::ST_25S03, {"25S03", "Transaction is rolled back"}},
    {SQLStateType::ST_28000, {"28000", "Invalid authorization specification"}},
    {SQLStateType::ST_34000, {"34000", "Invalid cursor name"}},
    {SQLStateType::ST_3C000, {"3C000", "Duplicate cursor name"}},
    {SQLStateType::ST_3D000, {"3D000", "Invalid catalog name"}},
    {SQLStateType::ST_3F000, {"3F000", "Invalid schema name"}},
    {SQLStateType::ST_40001, {"40001", "Serialization failure"}},
    {SQLStateType::ST_40002, {"40002", "Integrity constraint violation"}},
    {SQLStateType::ST_40003, {"40003", "Statement completion unknown"}},
    {SQLStateType::ST_42000, {"42000", "Syntax error or access violation"}},
    {SQLStateType::ST_42S01, {"42S01", "Base table or view already exists"}},
    {SQLStateType::ST_42S02, {"42S02", "Base table or view not found"}},
    {SQLStateType::ST_42S11, {"42S11", "Index already exists"}},
    {SQLStateType::ST_42S12, {"42S12", "Index not found"}},
    {SQLStateType::ST_42S21, {"42S21", "Column already exists"}},
    {SQLStateType::ST_42S22, {"42S22", "Column not found"}},
    {SQLStateType::ST_44000, {"44000", "WITH CHECK OPTION violation"}},
    {SQLStateType::ST_HY000, {"HY000", "General error"}},
    {SQLStateType::ST_HY001, {"HY001", "Memory allocation error"}},
    {SQLStateType::ST_HY003, {"HY003", "Invalid application buffer type"}},
    {SQLStateType::ST_HY004, {"HY004", "Invalid SQL data type"}},
    {SQLStateType::ST_HY007, {"HY007", "Associated statement is not prepared"}},
    {SQLStateType::ST_HY008, {"HY008", "Operation canceled"}},
    {SQLStateType::ST_HY009, {"HY009", "Invalid argument value"}},
    {SQLStateType::ST_HY010, {"HY010", "Function sequence error"}},
    {SQLStateType::ST_HY011, {"HY011", "Attribute cannot be set now"}},
    {SQLStateType::ST_HY012, {"HY012", "Invalid transaction operation code"}},
    {SQLStateType::ST_HY013, {"HY013", "Memory management error"}},
    {SQLStateType::ST_HY014, {"HY014", "Limit on the number of handles exceeded"}},
    {SQLStateType::ST_HY015, {"HY015", "No cursor name available"}},
    {SQLStateType::ST_HY016, {"HY016", "Cannot modify an implementation row descriptor"}},
    {SQLStateType::ST_HY017, {"HY017", "Invalid use of an automatically allocated descriptor handle"}},
    {SQLStateType::ST_HY018, {"HY018", "Server declined cancel request"}},
    {SQLStateType::ST_HY019, {"HY019", "Non-character and non-binary data sent in pieces"}},
    {SQLStateType::ST_HY020, {"HY020", "Attempt to concatenate a null value"}},
    {SQLStateType::ST_HY021, {"HY021", "Inconsistent descriptor information"}},
    {SQLStateType::ST_HY024, {"HY024", "Invalid attribute value"}},
    {SQLStateType::ST_HY090, {"HY090", "Invalid string or buffer length"}},
    {SQLStateType::ST_HY091, {"HY091", "Invalid descriptor field identifier"}},
    {SQLStateType::ST_HY092, {"HY092", "Invalid attribute/option identifier"}},
    {SQLStateType::ST_HY095, {"HY095", "Function type out of range"}},
    {SQLStateType::ST_HY096, {"HY096", "Information type out of range"}},
    {SQLStateType::ST_HY097, {"HY097", "Column type out of range"}},
    {SQLStateType::ST_HY098, {"HY098", "Scope type out of range"}},
    {SQLStateType::ST_HY099, {"HY099", "Nullable type out of range"}},
    {SQLStateType::ST_HY100, {"HY100", "Uniqueness option type out of range"}},
    {SQLStateType::ST_HY101, {"HY101", "Accuracy option type out of range"}},
    {SQLStateType::ST_HY103, {"HY103", "Invalid retrieval code"}},
    {SQLStateType::ST_HY104, {"HY104", "Invalid precision or scale value"}},
    {SQLStateType::ST_HY105, {"HY105", "Invalid parameter type"}},
    {SQLStateType::ST_HY106, {"HY106", "Fetch type out of range"}},
    {SQLStateType::ST_HY107, {"HY107", "Row value out of range"}},
    {SQLStateType::ST_HY109, {"HY109", "Invalid cursor position"}},
    {SQLStateType::ST_HY110, {"HY110", "Invalid driver completion"}},
    {SQLStateType::ST_HY111, {"HY111", "Invalid bookmark value"}},
    {SQLStateType::ST_HY114, {"HY114", "Driver does not support connection-level asynchronous function execution"}},
    {SQLStateType::ST_HY115,
     {"HY115", "SQLEndTran is not allowed for an environment that contains a connection with asynchronous function "
               "execution enabled"}},
    {SQLStateType::ST_HY117,
     {"HY117", "Connection is suspended due to unknown transaction state. Only disconnect and read-only functions are "
               "allowed."}},
    {SQLStateType::ST_HY121, {"HY121", "Cursor Library and Driver-Aware Pooling cannot be enabled at the same time"}},
    {SQLStateType::ST_HYC00, {"HYC00", "Optional feature not implemented"}},
    {SQLStateType::ST_HYT00, {"HYT00", "Timeout expired"}},
    {SQLStateType::ST_HYT01, {"HYT01", "Connection timeout expired"}},
    {SQLStateType::ST_IM001, {"IM001", "Driver does not support this function"}},
    {SQLStateType::ST_IM002, {"IM002", "Data source not found and no default driver specified"}},
    {SQLStateType::ST_IM003, {"IM003", "Specified driver could not be connected to"}},
    {SQLStateType::ST_IM004, {"IM004", "Driver's SQLAllocHandle on SQL_HANDLE_ENV failed"}},
    {SQLStateType::ST_IM005, {"IM005", "Driver's SQLAllocHandle on SQL_HANDLE_DBC failed"}},
    {SQLStateType::ST_IM006, {"IM006", "Driver's SQLSetConnectAttr failed"}},
    {SQLStateType::ST_IM007, {"IM007", "No data source or driver specified; dialog prohibited"}},
    {SQLStateType::ST_IM008, {"IM008", "Dialog failed"}},
    {SQLStateType::ST_IM009, {"IM009", "Unable to connect to translation DLL"}},
    {SQLStateType::ST_IM010, {"IM010", "Data source name too long"}},
    {SQLStateType::ST_IM011, {"IM011", "Driver name too long"}},
    {SQLStateType::ST_IM012, {"IM012", "DRIVER keyword syntax error"}},
    {SQLStateType::ST_IM014,
     {"IM014", "The specified DSN contains an architecture mismatch between the Driver and Application"}},
    {SQLStateType::ST_IM015, {"IM015", "Driver's SQLConnect on SQL_HANDLE_DBC_INFO_HANDLE failed"}},
    {SQLStateType::ST_IM017, {"IM017", "Polling is disabled in asynchronous notification mode"}},
    {SQLStateType::ST_IM018,
     {"IM018", "SQLCompleteAsync has not been called to complete the previous asynchronous operation on this handle."}},
    {SQLStateType::ST_S1118, {"S1118", "Driver does not support asynchronous notification"}}};

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
