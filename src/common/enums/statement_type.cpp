#include "common/enums/statement_type.hpp"

using namespace std;

namespace duckdb {

string StatementTypeToString(StatementType type) {
	switch(type) {
	case StatementType::SELECT:
		return "SELECT";
	case StatementType::INSERT:
		return "INSERT";
	case StatementType::UPDATE:
		return "UPDATE";
	case StatementType::DELETE:
		return "DELETE";
	case StatementType::DEALLOCATE:
		return "DEALLOCATE";
	case StatementType::PREPARE:
		return "PREPARE";
	case StatementType::EXECUTE:
		return "EXECUTE";
	case StatementType::ALTER:
		return "ALTER";
	case StatementType::TRANSACTION:
		return "TRANSACTION";
	case StatementType::COPY:
		return "COPY";
	case StatementType::ANALYZE:
		return "ANALYZE";
	case StatementType::VARIABLE_SET:
		return "VARIABLE_SET";
	case StatementType::CREATE_FUNC:
		return "CREATE_FUNC";
	case StatementType::EXPLAIN:
		return "EXPLAIN";
	case StatementType::CREATE_TABLE:
		return "CREATE_TABLE";
	case StatementType::CREATE_SCHEMA:
		return "CREATE_SCHEMA";
	case StatementType::CREATE_INDEX:
		return "CREATE_INDEX";
	case StatementType::CREATE_VIEW:
		return "CREATE_VIEW";
	case StatementType::CREATE_SEQUENCE:
		return "CREATE_SEQUENCE";
	case StatementType::DROP_TABLE:
		return "DROP_TABLE";
	case StatementType::DROP_SCHEMA:
		return "DROP_SCHEMA";
	case StatementType::DROP_INDEX:
		return "DROP_INDEX";
	case StatementType::DROP_VIEW:
		return "DROP_VIEW";
	case StatementType::DROP_SEQUENCE:
		return "DROP_SEQUENCE";
	default:
		assert(0);
		return "INVALID";
	}
}

}
