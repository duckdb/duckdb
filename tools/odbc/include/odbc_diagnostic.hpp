#ifndef ODBC_DIAGNOSTIC_HPP
#define ODBC_DIAGNOSTIC_HPP

#include "duckdb.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/common/enum_class_hash.hpp"

#include "sqlext.h"
#include "sqltypes.h"

#include <set>
#include <stack>
#include <string>
#include <vector>
#include <unordered_map>

namespace duckdb {
enum class SQLStateType : uint8_t {
	GENERAL_WARNING = 0,             //    {"01000", "General warning"},
	CURSOR_CONFLICT = 1,             //    {"01001", "Cursor operation conflict"},
	DISCONNECT_ERROR = 2,            //    {"01002", "Disconnect error"},
	NULL_VALUE_ELIM = 3,             //    {"01003", "NULL value eliminated in set function"},
	STR_RIGHT_TRUNCATE = 4,          //    {"01004", "String data, right truncated"},
	PRIVILEGE_NOT_REVOKED = 5,       //    {"01006", "Privilege not revoked"},
	PRIVILEGE_NOT_GRANTED = 6,       //    {"01007", "Privilege not granted"},
	INVALID_CONNECTION_STR_ATTR = 7, //    {"01S00", "Invalid connection string attribute"},
	ERROR_ROW = 8,                   //     {"01S01", "Error in row"},
	OPTION_VALUE_CHANGED = 9,        //    {"01S02", "Option value changed"},
	FETCH_BEFORE_FIRST_RESULT_SET =
	    10,                       //    {"01S06", "Attempt to fetch before the result set returned the first rowset"},
	FRACTIONAL_TRUNCATE = 12,     //    {"01S07", "Fractional truncation"},
	ERROR_SAVE_DSN_FILE = 13,     //    {"01S08", "Error saving file DSN"},
	INVALID_KEYWORD = 14,         //    {"01S09", "Invalid keyword"},
	COUNT_FIELD_INCORRECT = 15,   //    {"07002", "COUNT field incorrect"},
	PREPARE_STMT_NO_CURSOR = 16,  //    {"07005", "Prepared statement not a cursor-specification"},
	RESTRICTED_DATA_TYPE = 17,    //    {"07006", "Restricted data type attribute violation"},
	RETRICT_PARAMETER_VALUE = 18, //    {"07007", "Restricted parameter value violation"},
	INVALID_DESC_INDEX = 19,      //    {"07009", "Invalid descriptor index"},
	INVALID_USE_DEFAULT_PARAMETER = 20,     //	{"07S01", "Invalid use of default parameter"},
	CLIENT_UNABLE_TO_CONNECT = 21,          //	{"08001", "Client unable to establish connection"},
	CONNECTION_NAME_IN_USE = 22,            //    {"08002", "Connection name in use"},
	CONNECTION_NOT_OPEN = 23,               //    {"08003", "Connection not open"},
	SERVER_REJECT_CONNECTION = 24,          //	{"08004", "Server rejected the connection"},
	CONNECTION_FAIL_TRANSACTION = 25,       //    {"08007", "Connection failure during transaction"},
	LINK_FAILURE = 26,                      //    {"08S01", "Communication link failure"},
	FEATURE_NOT_SUPPORTED = 27,             //    {"0A000", "Feature not supported"},
	LIST_INSERT_MATCH_ERROR = 28,           //    {"21S01", "Insert value list does not match column list"},
	DERIVED_TABLE_MATCH_ERROR = 29,         //	{"21S02", "Degree of derived table does not match column list"},
	STR_RIGHT_TRUNCATE2 = 30,               // {"22001", "String data, right truncated"},
	INDICATOR_VARIABLE_NOT_SUPPLIED = 31,   //    {"22002", "Indicator variable required but not supplied"},
	NUMERIC_OUT_RANGE = 32,                 //	{"22003", "Numeric value out of range"},
	INVALID_DATATIME_FORMAT = 33,           //	{"22007", "Invalid datetime format"},
	DATATIME_OVERVLOW = 34,                 //    {"22008", "Datetime field overflow"},
	DIVISION_BY_ZERO = 35,                  //    {"22012", "Division by zero"},
	INTERVAL_OVERFLOW = 36,                 //	{"22015", "Interval field overflow"},
	INVALID_CAST_CHAR = 37,                 //	{"22018", "Invalid character value for cast specification"},
	INVALID_ESCAPE_CHAR = 38,               //	{"22019", "Invalid escape character"},
	INVALID_ESCAPE_SEQ = 39,                //	{"22025", "Invalid escape sequence"},
	STR_LEN_MISMATCH = 40,                  //	{"22026", "String data, length mismatch"},
	CONSTRAINT_VIOLATION = 41,              //    {"23000", "Integrity constraint violation"},
	INVALID_CURSOR_STATE = 42,              //	{"24000", "Invalid cursor state"},
	INVALID_TRANSACTION_STATE = 43,         //	{"25000", "Invalid transaction state"},
	TRANSACTION_STATE_UNKNOWN = 44,         //	{"25S01", "Transaction state unknown"},
	TRANSACTION_STILL_ACTIVE = 45,          //	{"25S02", "Transaction is still active"},
	TRANSACTION_ROLLED_BACK = 46,           //	{"25S03", "Transaction is rolled back"},
	INVALID_AUTH = 47,                      //	{"28000", "Invalid authorization specification"},
	INVALID_CURSOR_NAME = 48,               //	{"34000", "Invalid cursor name"},
	DUPLICATE_CURSOR_NAME = 49,             //	{"3C000", "Duplicate cursor name"},
	INVALID_CATALOG_NAME = 50,              //	{"3D000", "Invalid catalog name"},
	INVALID_SCHEMA_NAME = 51,               //	{"3F000", "Invalid schema name"},
	SERIALIZATION_FAILURE = 52,             //	{"40001", "Serialization failure"},
	CONSTRAINT_VIOLATION2 = 53,             // {"40002", "Integrity constraint violation"},
	STMT_COMPLETION_UNKNOWN = 54,           //	{"40003", "Statement completion unknown"},
	SYNTAX_ERROR_OR_ACCESS_VIOLATION = 55,  //	{"42000", "Syntax error or access violation"},
	TABLE_OR_VIEW_ALREADY_EXIST = 56,       // {"42S01", "Base table or view already exists"},
	TABLE_OR_VIEW_NOT_FOUND = 57,           // {"42S02", "Base table or view not found"},
	INDEX_ALREADY_EXIST = 58,               // {"42S11", "Index already exists"},
	INDEX_NOT_FOUND = 59,                   // {"42S12", "Index not found"},
	COLUMN_ALREADY_EXIST = 60,              // {"42S21", "Column already exists"},
	COLUMN_NOT_FOUND = 61,                  // {"42S22", "Column not found"},
	WITH_CHECK_POINT_VIOLARION = 62,        // {"44000", "WITH CHECK OPTION violation"},
	GENERAL_ERROR = 63,                     // {"HY000", "General error"},
	MEMORY_ALLOCATION_ERROR = 64,           // {"HY001", "Memory allocation error"},
	INVALID_APP_BUFFER_TYPE = 65,           // {"HY003", "Invalid application buffer type"},
	INVALD_SQL_TYPE = 66,                   // {"HY004", "Invalid SQL data type"},
	STMT_NOT_PREPARED = 67,                 // {"HY007", "Associated statement is not prepared"},
	OPERATION_CANCELLED = 68,               // {"HY008", "Operation canceled"},
	INVALIDE_ARGUMENT = 69,                 // {"HY009", "Invalid argument value"},
	FUNCTION_SEQ_ERROR = 70,                // {"HY010", "Function sequence error"},
	ATTR_CANNOT_BE_SET_NOW = 71,            // {"HY011", "Attribute cannot be set now"},
	INVALID_TRANSACTION_OP_CODE = 72,       // {"HY012", "Invalid transaction operation code"},
	MEMORY_MANAGEMENT_ERROR = 73,           // {"HY013", "Memory management error"},
	NUMBER_HANDLES_EXCEEDED = 74,           // {"HY014", "Limit on the number of handles exceeded"},
	NO_CURSOR_NAME_AVAILABLE = 75,          // {"HY015", "No cursor name available"},
	CANNOT_MODIFY_IRD = 76,                 // {"HY016", "Cannot modify an implementation row descriptor"},
	INVALID_USE_AUTO_ALLOC_DESCRIPTOR = 77, // {"HY017", "Invalid use of an automatically allocated descriptor handle"},
	SERVER_DECLINED_CANCEL_REQUEST = 78,    // {"HY018", "Server declined cancel request"},
	NON_CHAR_BIN_SENT_IN_PIECES = 79,       // {"HY019", "Non-character and non-binary data sent in pieces"},
	ATTEMPT_CONCAT_NULL_VALUE = 80,         // {"HY020", "Attempt to concatenate a null value"},
	INCONSISTENT_DESC_INFO = 81,            // {"HY021", "Inconsistent descriptor information"},
	INVALID_ATTR_VALUE = 82,                // {"HY024", "Invalid attribute value"},
	INVALID_STR_BUFF_LENGTH = 83,           // {"HY090", "Invalid string or buffer length"},
	INVALD_DESC_FIELD_ID = 84,              // {"HY091", "Invalid descriptor field identifier"},
	INVALID_ATTR_OPTION_ID = 85,            // {"HY092", "Invalid attribute/option identifier"},
	FUNCTION_TYPE_OUT_RANGE = 86,           // {"HY095", "Function type out of range"},
	INFO_TYPE_OUT_RANGE = 87,               // {"HY096", "Information type out of range"},
	COLUMN_TYPE_OUT_RANGE = 88,             // {"HY097", "Column type out of range"},
	SCOPE_TYPE_OUT_RANGE = 89,              // {"HY098", "Scope type out of range"},
	NULL_TYPE_OUT_RANGE = 90,               // {"HY099", "Nullable type out of range"},
	UNIQ_OPTION_TYPE_OUT_RANGE = 91,        // {"HY100", "Uniqueness option type out of range"},
	ACCURARY_OPTION_TYPE_OUT_RANGE = 92,    // {"HY101", "Accuracy option type out of range"},
	INVALID_RETRIEVAL_CODE = 93,            // {"HY103", "Invalid retrieval code"},
	INVALID_PREC_SCALE_TYPE = 94,           // {"HY104", "Invalid precision or scale value"},
	INVALID_PARAMETER_TYPE = 95,            // {"HY105", "Invalid parameter type"},
	FETCH_TYPE_OUT_RANGE = 96,              // {"HY106", "Fetch type out of range"},
	ROW_VALUE_OUT_RANGE = 97,               // {"HY107", "Row value out of range"},
	INVALID_CURSOR_POS = 98,                // {"HY109", "Invalid cursor position"},
	INVALID_DRIVER_COMPLETION = 99,         // {"HY110", "Invalid driver completion"},
	INVALID_BOOKMARK_VALUE = 100,           // {"HY111", "Invalid bookmark value"},
	NOT_SUPPORT_ASYNC_FUNCT_EXECUTION =
	    101, // {"HY114", "Driver does not support connection-level asynchronous function execution"},
	SQLENDTRAN_ASYNC_FUNCT_EXECUTION = 102, // {"HY115", "SQLEndTran is not allowed for an environment that contains a
	                                        // connection with asynchronous function execution enabled"},
	CONNECTION_UNKNOW_TRANSACTION_STATE = 103, // {"HY117", "Connection is suspended due to unknown transaction state.
	                                           // Only disconnect and read-only functions are allowed."},
	CURSOR_DRIVER_POOLING_SAME_TIME =
	    104, // {"HYHY121", "Cursor Library and Driver-Aware Pooling cannot be enabled at the same time"},
	OPT_FEATURE_NOT_IMPLEMENTED = 105,       // {"HYC00", "Optional feature not implemented"},
	TIMEOUT_EXPIRED = 106,                   // {"HYT00", "Timeout expired"},
	CONNECTION_TIMEOUT_EXPIRED = 107,        // {"HYT01", "Connection timeout expired"},
	DRIVER_NOT_SUPPORT_FUNCTION = 108,       // {"IM001", "Driver does not support this function"},
	DSN_NOT_FOUND_NO_DEFAULT = 109,          // {"IM002", "Data source not found and no default driver specified"},
	DRIVER_COULD_NOT_BE_CONNECTED = 110,     // {"IM003", "Specified driver could not be connected to"},
	ALLOC_HANDLE_ENV_FAIL = 111,             // {"IM004", "Driver's SQLAllocHandle on SQL_HANDLE_ENV failed"},
	ALLOC_HANDLE_DBC_FAIL = 112,             // {"IM005", "Driver's SQLAllocHandle on SQL_HANDLE_DBC failed"},
	SET_CONNECTION_ATTR_FAIL = 113,          // {"IM006", "Driver's SQLSetConnectAttr failed"},
	NO_DSN_OR_DRIVER = 114,                  // {"IM007", "No data source or driver specified; dialog prohibited"},
	DIALOG_FAIL = 115,                       // {"IM008", "Dialog failed"},
	UNABLE_CONNECT_TO_TRANSLATION_DLL = 116, // {"IM009", "Unable to connect to translation DLL"},
	DSN_TOO_LONG = 117,                      // {"IM010", "Data source name too long"},
	DRIVER_NAME_TOO_LONG = 118,              // {"IM011", "Driver name too long"},
	KEYWORD_SYNTAX_ERROR = 119,              // {"IM012", "DRIVER keyword syntax error"},
	DNS_ARCHITECTURE_MISMATCH =
	    120, // {"IM014", "The specified DSN contains an architecture mismatch between the Driver and Application"},
	CONNECT_HANDLE_DBC_INFO_FAIL = 121, // {"IM015", "Driver's SQLConnect on SQL_HANDLE_DBC_INFO_HANDLE failed"},
	POLLING_DISABLED_ACYNC_NOTIGICATION_MODE =
	    122,                             // {"IM017", "Polling is disabled in asynchronous notification mode"},
	SHOULD_CALL_COMPLETE_ASYNC = 123,    // {"IM018", "SQLCompleteAsync has not been called to complete the previous
	                                     // asynchronous operation on this handle."},
	NOT_SUPPORT_ASYNC_NOTIFICATION = 124 // {"S1118", "Driver does not support asynchronous notification"};
};

struct SQLState {
	std::string code;
	std::string erro_msg;
};

struct DiagRecord {
public:
	explicit DiagRecord(const std::string &msg, const SQLStateType &sqlstate_type, const std::string &server_name = "",
	                    SQLINTEGER col_number = SQL_NO_COLUMN_NUMBER, SQLINTEGER sql_native = 0,
	                    SQLLEN row_number = SQL_NO_ROW_NUMBER);
	DiagRecord(const DiagRecord &other);
	DiagRecord &operator=(const DiagRecord &other);

	std::string GetOriginalMessage();
	std::string GetMessage(SQLSMALLINT buff_length);
	void SetMessage(const std::string &new_msg);
	void ClearStackMsgOffset();

public:
	// Some fields were commented out because they can be extract from other fields or internal data structures
	// std::string sql_diag_class_origin;
	SQLINTEGER sql_diag_column_number = SQL_NO_COLUMN_NUMBER;
	// std::string sql_diag_connection_name;
	SQLINTEGER sql_diag_native = 0;
	SQLLEN sql_diag_row_number = SQL_NO_ROW_NUMBER;
	std::string sql_diag_server_name;
	std::string sql_diag_sqlstate;
	// std::string sql_diag_subclass_origin;
	// default id_diag
	std::string sql_diag_message_text;
	std::stack<duckdb::idx_t> stack_msg_offset;
};

struct DiagHeader {
public:
	SQLLEN sql_diag_cursor_row_count;
	// std::string sql_diag_dynamic_function; // this field is extract from map_dynamic_function
	SQLINTEGER sql_diag_dynamic_function_code = SQL_DIAG_UNKNOWN_STATEMENT;
	SQLINTEGER sql_diag_number;
	SQLRETURN sql_diag_return_code;
	SQLLEN sql_diag_row_count;
};

class OdbcDiagnostic {
public:
	DiagHeader header;
	std::vector<DiagRecord> diag_records;
	// vector that mantains the indexes of DiagRecord
	std::vector<SQLSMALLINT> vec_record_idx;
	static const std::unordered_map<SQLINTEGER, std::string> MAP_DYNAMIC_FUNCTION;
	static const std::set<std::string> SET_ODBC3_SUBCLASS_ORIGIN;
	static const std::unordered_map<SQLStateType, SQLState, duckdb::EnumClassHash> MAP_ODBC_SQL_STATES;

public:
	static bool IsDiagRecordField(SQLSMALLINT diag_identifier);

	void FormatDiagnosticMessage(DiagRecord &diag_record, const std::string &data_source, const std::string &component);
	void AddDiagRecord(DiagRecord &diag_record);
	void AddNewRecIdx(SQLSMALLINT rec_idx);
	duckdb::idx_t GetTotalRecords();

	void Clean();

	std::string GetDiagDynamicFunction();
	bool VerifyRecordIndex(SQLINTEGER rec_idx);
	DiagRecord &GetDiagRecord(SQLINTEGER rec_idx);
	std::string GetDiagClassOrigin(SQLINTEGER rec_idx);
	std::string GetDiagSubclassOrigin(SQLINTEGER rec_idx);
};

} // namespace duckdb
#endif
