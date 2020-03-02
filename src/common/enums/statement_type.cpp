#include "duckdb/common/enums/statement_type.hpp"

using namespace std;

namespace duckdb {

string StatementTypeToString(StatementType type) {
	switch (type) {
	case StatementType::SELECT:
		return "SELECT";
	case StatementType::INSERT:
		return "INSERT";
	case StatementType::UPDATE:
		return "UPDATE";
	case StatementType::DELETE:
		return "DELETE";
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
	case StatementType::CREATE:
		return "CREATE";
	case StatementType::DROP:
		return "DROP";
	case StatementType::PRAGMA:
		return "PRAGMA";
	default:
		return "INVALID";
	}
}

} // namespace duckdb
