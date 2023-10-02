#ifndef ODBC_DIAGNOSTIC_HPP
#define ODBC_DIAGNOSTIC_HPP

#include "duckdb.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/common/enum_class_hash.hpp"

#include "sqlext.h"
#include "sqltypes.h"

#include "duckdb/common/string.hpp"
#include "duckdb/common/stack.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
enum class SQLStateType : uint8_t {
	ST_01000, // {"01000", "General warning"},
	ST_01001, // {"01001", "Cursor operation conflict"},
	ST_01002, // {"01002", "Disconnect error"},
	ST_01003, // {"01003", "NULL value eliminated in set function"},
	ST_01004, // {"01004", "String data, right truncated"},
	ST_01006, // {"01006", "Privilege not revoked"},
	ST_01007, // {"01007", "Privilege not granted"},
	ST_01S00, // {"01S00", "Invalid connection string attribute"},
	ST_01S01, // {"01S01", "Error in row"},
	ST_01S02, // {"01S02", "Option value changed"},
	ST_01S06, // {"01S06", "Attempt to fetch before the result set returned the first rowset"},
	ST_01S07, // {"01S07", "Fractional truncation"},
	ST_01S08, // {"01S08", "Error saving file DSN"},
	ST_01S09, // {"01S09", "Invalid keyword"},
	ST_07002, // {"07002", "COUNT field incorrect"},
	ST_07005, // {"07005", "Prepared statement not a cursor-specification"},
	ST_07006, // {"07006", "Restricted data type attribute violation"},
	ST_07007, // {"07007", "Restricted parameter value violation"},
	ST_07009, // {"07009", "Invalid descriptor index"},
	ST_07S01, // {"07S01", "Invalid use of default parameter"},
	ST_08001, // {"08001", "Client unable to establish connection"},
	ST_08002, // {"08002", "Connection name in use"},
	ST_08003, // {"08003", "Connection not open"},
	ST_08004, // {"08004", "Server rejected the connection"},
	ST_08007, // {"08007", "Connection failure during transaction"},
	ST_08S01, // {"08S01", "Communication link failure"},
	ST_0A000, // {"0A000", "Feature not supported"},
	ST_21S01, // {"21S01", "Insert value list does not match column list"},
	ST_21S02, // {"21S02", "Degree of derived table does not match column list"},
	ST_22001, // {"22001", "String data, right truncated"},
	ST_22002, // {"22002", "Indicator variable required but not supplied"},
	ST_22003, // {"22003", "Numeric value out of range"},
	ST_22007, // {"22007", "Invalid datetime format"},
	ST_22008, // {"22008", "Datetime field overflow"},
	ST_22012, // {"22012", "Division by zero"},
	ST_22015, // {"22015", "Interval field overflow"},
	ST_22018, // {"22018", "Invalid character value for cast specification"},
	ST_22019, // {"22019", "Invalid escape character"},
	ST_22025, // {"22025", "Invalid escape sequence"},
	ST_22026, // {"22026", "String data, length mismatch"},
	ST_23000, // {"23000", "Integrity constraint violation"},
	ST_24000, // {"24000", "Invalid cursor state"},
	ST_25000, // {"25000", "Invalid transaction state"},
	ST_25S01, // {"25S01", "Transaction state unknown"},
	ST_25S02, // {"25S02", "Transaction is still active"},
	ST_25S03, // {"25S03", "Transaction is rolled back"},
	ST_28000, // {"28000", "Invalid authorization specification"},
	ST_34000, // {"34000", "Invalid cursor name"},
	ST_3C000, // {"3C000", "Duplicate cursor name"},
	ST_3D000, // {"3D000", "Invalid catalog name"},
	ST_3F000, // {"3F000", "Invalid schema name"},
	ST_40001, // {"40001", "Serialization failure"},
	ST_40002, // {"40002", "Integrity constraint violation"},
	ST_40003, // {"40003", "Statement completion unknown"},
	ST_42000, // {"42000", "Syntax error or access violation"},
	ST_42S01, // {"42S01", "Base table or view already exists"},
	ST_42S02, // {"42S02", "Base table or view not found"},
	ST_42S11, // {"42S11", "Index already exists"},
	ST_42S12, // {"42S12", "Index not found"},
	ST_42S21, // {"42S21", "Column already exists"},
	ST_42S22, // {"42S22", "Column not found"},
	ST_44000, // {"44000", "WITH CHECK OPTION violation"},
	ST_HY000, // {"HY000", "General error"},
	ST_HY001, // {"HY001", "Memory allocation error"},
	ST_HY003, // {"HY003", "Invalid application buffer type"},
	ST_HY004, // {"HY004", "Invalid SQL data type"},
	ST_HY007, // {"HY007", "Associated statement is not prepared"},
	ST_HY008, // {"HY008", "Operation canceled"},
	ST_HY009, // {"HY009", "Invalid argument value"},
	ST_HY010, // {"HY010", "Function sequence error"},
	ST_HY011, // {"HY011", "Attribute cannot be set now"},
	ST_HY012, // {"HY012", "Invalid transaction operation code"},
	ST_HY013, // {"HY013", "Memory management error"},
	ST_HY014, // {"HY014", "Limit on the number of handles exceeded"},
	ST_HY015, // {"HY015", "No cursor name available"},
	ST_HY016, // {"HY016", "Cannot modify an implementation row descriptor"},
	ST_HY017, // {"HY017", "Invalid use of an automatically allocated descriptor handle"},
	ST_HY018, // {"HY018", "Server declined cancel request"},
	ST_HY019, // {"HY019", "Non-character and non-binary data sent in pieces"},
	ST_HY020, // {"HY020", "Attempt to concatenate a null value"},
	ST_HY021, // {"HY021", "Inconsistent descriptor information"},
	ST_HY024, // {"HY024", "Invalid attribute value"},
	ST_HY090, // {"HY090", "Invalid string or buffer length"},
	ST_HY091, // {"HY091", "Invalid descriptor field identifier"},
	ST_HY092, // {"HY092", "Invalid attribute/option identifier"},
	ST_HY095, // {"HY095", "Function type out of range"},
	ST_HY096, // {"HY096", "Information type out of range"},
	ST_HY097, // {"HY097", "Column type out of range"},
	ST_HY098, // {"HY098", "Scope type out of range"},
	ST_HY099, // {"HY099", "Nullable type out of range"},
	ST_HY100, // {"HY100", "Uniqueness option type out of range"},
	ST_HY101, // {"HY101", "Accuracy option type out of range"},
	ST_HY103, // {"HY103", "Invalid retrieval code"},
	ST_HY104, // {"HY104", "Invalid precision or scale value"},
	ST_HY105, // {"HY105", "Invalid parameter type"},
	ST_HY106, // {"HY106", "Fetch type out of range"},
	ST_HY107, // {"HY107", "Row value out of range"},
	ST_HY109, // {"HY109", "Invalid cursor position"},
	ST_HY110, // {"HY110", "Invalid driver completion"},
	ST_HY111, // {"HY111", "Invalid bookmark value"},
	ST_HY114, // {"HY114", "Driver does not support connection-level asynchronous function execution"},
	ST_HY115, // {"HY115", "SQLEndTran is not allowed for an environment that contains a
	          // connection with asynchronous function execution enabled"},
	ST_HY117, // {"HY117", "Connection is suspended due to unknown transaction state.
	          // Only disconnect and read-only functions are allowed."},
	ST_HY121, // {"HY121", "Cursor Library and Driver-Aware Pooling cannot be enabled at the same time"},
	ST_HYC00, // {"HYC00", "Optional feature not implemented"},
	ST_HYT00, // {"HYT00", "Timeout expired"},
	ST_HYT01, // {"HYT01", "Connection timeout expired"},
	ST_IM001, // {"IM001", "Driver does not support this function"},
	ST_IM002, // {"IM002", "Data source not found and no default driver specified"},
	ST_IM003, // {"IM003", "Specified driver could not be connected to"},
	ST_IM004, // {"IM004", "Driver's SQLAllocHandle on SQL_HANDLE_ENV failed"},
	ST_IM005, // {"IM005", "Driver's SQLAllocHandle on SQL_HANDLE_DBC failed"},
	ST_IM006, // {"IM006", "Driver's SQLSetConnectAttr failed"},
	ST_IM007, // {"IM007", "No data source or driver specified; dialog prohibited"},
	ST_IM008, // {"IM008", "Dialog failed"},
	ST_IM009, // {"IM009", "Unable to connect to translation DLL"},
	ST_IM010, // {"IM010", "Data source name too long"},
	ST_IM011, // {"IM011", "Driver name too long"},
	ST_IM012, // {"IM012", "DRIVER keyword syntax error"},
	ST_IM014, // {"IM014", "The specified DSN contains an architecture mismatch between the Driver and Application"},
	ST_IM015, // {"IM015", "Driver's SQLConnect on SQL_HANDLE_DBC_INFO_HANDLE failed"},
	ST_IM017, // {"IM017", "Polling is disabled in asynchronous notification mode"},
	ST_IM018, // {"IM018", "SQLCompleteAsync has not been called to complete the previous
	          // asynchronous operation on this handle."},
	ST_S1118  // {"S1118", "Driver does not support asynchronous notification"};
};

struct SQLState {
	std::string code;
	std::string error_msg;
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
	// Some fields were commented out because they can be extracted from other fields or internal data structures
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
	vector<DiagRecord> diag_records;
	// vector that mantains the indexes of DiagRecord
	vector<SQLSMALLINT> vec_record_idx;
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
