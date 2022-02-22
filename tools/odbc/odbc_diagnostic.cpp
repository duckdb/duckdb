#include "odbc_diagnostic.hpp"

using duckdb::DiagRecord;
using duckdb::OdbcDiagnostic;
using std::string;

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

const std::unordered_map<std::string, std::string> OdbcDiagnostic::MAP_ODBC_SQL_STATES = {
    {"01000", "General warning"},
    {"01001", "Cursor operation conflict"},
    {"01002", "Disconnect error"},
    {"01003", "NULL value eliminated in set function"},
    {"01004", "String data, right truncated"},
    {"01006", "Privilege not revoked"},
    {"01007", "Privilege not granted"},
    {"01S00", "Invalid connection string attribute"},
    {"01S01", "Error in row"},
    {"01S02", "Option value changed"},
    {"01S06", "Attempt to fetch before the result set returned the first "
              "rowset"},
    {"01S07", "Fractional truncation"},
    {"01S08", "Error saving file DSN"},
    {"01S09", "Invalid keyword"},
    {"07002", "COUNT field incorrect"},
    {"07005", "Prepared statement not a cursor-specification"},
    {"07006", "Restricted data type attribute violation"},
    {"07007", "Restricted parameter value violation"},
    {"07009", "Invalid descriptor index"},
    {"07S01", "Invalid use of default parameter"},
    {"08001", "Client unable to establish connection"},
    {"08002", "Connection name in use"},
    {"08003", "Connection not open"},
    {"08004", "Server rejected the connection"},
    {"08007", "Connection failure during transaction"},
    {"08S01", "Communication link failure"},
    {"0A000", "Feature not supported"},
    {"21S01", "Insert value list does not match column list"},
    {"21S02", "Degree of derived table does not match column list"},
    {"22001", "String data, right truncated"},
    {"22002", "Indicator variable required but not supplied"},
    {"22003", "Numeric value out of range"},
    {"22007", "Invalid datetime format"},
    {"22008", "Datetime field overflow"},
    {"22012", "Division by zero"},
    {"22015", "Interval field overflow"},
    {"22018", "Invalid character value for cast specification"},
    {"22019", "Invalid escape character"},
    {"22025", "Invalid escape sequence"},
    {"22026", "String data, length mismatch"},
    {"23000", "Integrity constraint violation"},
    {"24000", "Invalid cursor state"},
    {"25000", "Invalid transaction state"},
    {"25S01", "Transaction state unknown"},
    {"25S02", "Transaction is still active"},
    {"25S03", "Transaction is rolled back"},
    {"28000", "Invalid authorization specification"},
    {"34000", "Invalid cursor name"},
    {"3C000", "Duplicate cursor name"},
    {"3D000", "Invalid catalog name"},
    {"3F000", "Invalid schema name"},
    {"40001", "Serialization failure"},
    {"40002", "Integrity constraint violation"},
    {"40003", "Statement completion unknown"},
    {"42000", "Syntax error or access violation"},
    {"42S01", "Base table or view already exists"},
    {"42S02", "Base table or view not found"},
    {"42S11", "Index already exists"},
    {"42S12", "Index not found"},
    {"42S21", "Column already exists"},
    {"42S22", "Column not found"},
    {"44000", "WITH CHECK OPTION violation"},
    {"HY000", "General error"},
    {"HY001", "Memory allocation error"},
    {"HY003", "Invalid application buffer type"},
    {"HY004", "Invalid SQL data type"},
    {"HY007", "Associated statement is not prepared"},
    {"HY008", "Operation canceled"},
    {"HY009", "Invalid argument value"},
    {"HY010", "Function sequence error"},
    {"HY011", "Attribute cannot be set now"},
    {"HY012", "Invalid transaction operation code"},
    {"HY013", "Memory management error"},
    {"HY014", "Limit on the number of handles exceeded"},
    {"HY015", "No cursor name available"},
    {"HY016", "Cannot modify an implementation row descriptor"},
    {"HY017", "Invalid use of an automatically allocated descriptor "
              "handle"},
    {"HY018", "Server declined cancel request"},
    {"HY019", "Non-character and non-binary data sent in pieces"},
    {"HY020", "Attempt to concatenate a null value"},
    {"HY021", "Inconsistent descriptor information"},
    {"HY024", "Invalid attribute value"},
    {"HY090", "Invalid string or buffer length"},
    {"HY091", "Invalid descriptor field identifier"},
    {"HY092", "Invalid attribute/option identifier"},
    {"HY095", "Function type out of range"},
    {"HY096", "Information type out of range"},
    {"HY097", "Column type out of range"},
    {"HY098", "Scope type out of range"},
    {"HY099", "Nullable type out of range"},
    {"HY100", "Uniqueness option type out of range"},
    {"HY101", "Accuracy option type out of range"},
    {"HY103", "Invalid retrieval code"},
    {"HY104", "Invalid precision or scale value"},
    {"HY105", "Invalid parameter type"},
    {"HY106", "Fetch type out of range"},
    {"HY107", "Row value out of range"},
    {"HY109", "Invalid cursor position"},
    {"HY110", "Invalid driver completion"},
    {"HY111", "Invalid bookmark value"},
    {"HY114", "Driver does not support connection-level asynchronous "
              "function execution"},
    {"HY115", "SQLEndTran is not allowed for an environment that contains "
              "a connection with asynchronous function execution enabled"},
    {"HY117", "Connection is suspended due to unknown transaction state.  "
              "Only disconnect and read-only functions are allowed."},
    {"HY121", "Cursor Library and Driver-Aware Pooling cannot be enabled "
              "at the same time"},
    {"HYC00", "Optional feature not implemented"},
    {"HYT00", "Timeout expired"},
    {"HYT01", "Connection timeout expired"},
    {"IM001", "Driver does not support this function"},
    {"IM002", "Data source not found and no default driver specified"},
    {"IM003", "Specified driver could not be connected to"},
    {"IM004", "Driver's SQLAllocHandle on SQL_HANDLE_ENV failed"},
    {"IM005", "Driver's SQLAllocHandle on SQL_HANDLE_DBC failed"},
    {"IM006", "Driver's SQLSetConnectAttr failed"},
    {"IM007", "No data source or driver specified; dialog prohibited"},
    {"IM008", "Dialog failed"},
    {"IM009", "Unable to connect to translation DLL"},
    {"IM010", "Data source name too long"},
    {"IM011", "Driver name too long"},
    {"IM012", "DRIVER keyword syntax error"},
    {"IM014", "The specified DSN contains an architecture mismatch "
              "between the Driver and Application"},
    {"IM015", "Driver's SQLConnect on SQL_HANDLE_DBC_INFO_HANDLE failed"},
    {"IM017", "Polling is disabled in asynchronous notification mode"},
    {"IM018", "SQLCompleteAsync has not been called to complete the "
              "previous asynchronous operation on this handle."},
    {"S1118", "Driver does not support asynchronous notification"}};

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

const DiagRecord &OdbcDiagnostic::GetDiagRecord(SQLINTEGER rec_idx) {
	D_ASSERT(rec_idx < (SQLINTEGER)diag_records.size() && rec_idx >= 0);
	return diag_records[rec_idx];
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
	if (SET_ODBC3_SUBCLASS_ORIGIN.find(sqlstate_str) != SET_ODBC3_SUBCLASS_ORIGIN.end()) {
		return "ODBC 3.0";
	} else {
		return "ISO 9075";
	}
}