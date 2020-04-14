#include "duckdb/common/enums/logical_operator_type.hpp"

using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// Value <--> String Utilities
//===--------------------------------------------------------------------===//
string LogicalOperatorToString(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::GET:
		return "GET";
	case LogicalOperatorType::CHUNK_GET:
		return "CHUNK_GET";
	case LogicalOperatorType::DELIM_GET:
		return "DELIM_GET";
	case LogicalOperatorType::EMPTY_RESULT:
		return "EMPTY_RESULT";
	case LogicalOperatorType::EXPRESSION_GET:
		return "EXPRESSION_GET";
	case LogicalOperatorType::ANY_JOIN:
		return "ANY_JOIN";
	case LogicalOperatorType::COMPARISON_JOIN:
		return "COMPARISON_JOIN";
	case LogicalOperatorType::DELIM_JOIN:
		return "DELIM_JOIN";
	case LogicalOperatorType::PROJECTION:
		return "PROJECTION";
	case LogicalOperatorType::FILTER:
		return "FILTER";
	case LogicalOperatorType::AGGREGATE_AND_GROUP_BY:
		return "AGGREGATE_AND_GROUP_BY";
	case LogicalOperatorType::WINDOW:
		return "WINDOW";
	case LogicalOperatorType::UNNEST:
		return "UNNEST";
	case LogicalOperatorType::LIMIT:
		return "LIMIT";
	case LogicalOperatorType::ORDER_BY:
		return "ORDER_BY";
	case LogicalOperatorType::TOP_N:
		return "TOP_N";
	case LogicalOperatorType::COPY_TO_FILE:
		return "COPY_TO_FILE";
	case LogicalOperatorType::COPY_FROM_FILE:
		return "COPY_FROM_FILE";
	case LogicalOperatorType::JOIN:
		return "JOIN";
	case LogicalOperatorType::CROSS_PRODUCT:
		return "CROSS_PRODUCT";
	case LogicalOperatorType::UNION:
		return "UNION";
	case LogicalOperatorType::EXCEPT:
		return "EXCEPT";
	case LogicalOperatorType::INTERSECT:
		return "INTERSECT";
	case LogicalOperatorType::INSERT:
		return "INSERT";
	case LogicalOperatorType::DISTINCT:
		return "DISTINCT";
	case LogicalOperatorType::DELETE:
		return "DELETE";
	case LogicalOperatorType::UPDATE:
		return "UPDATE";
	case LogicalOperatorType::PREPARE:
		return "PREPARE";
	case LogicalOperatorType::TABLE_FUNCTION:
		return "TABLE_FUNCTION";
	case LogicalOperatorType::CREATE_INDEX:
		return "CREATE_INDEX";
	case LogicalOperatorType::CREATE_TABLE:
		return "CREATE_TABLE";
	case LogicalOperatorType::EXPLAIN:
		return "EXPLAIN";
	case LogicalOperatorType::EXECUTE:
		return "EXECUTE";
	case LogicalOperatorType::VACUUM:
		return "VACUUM";
	case LogicalOperatorType::INDEX_SCAN:
		return "INDEX_SCAN";
	case LogicalOperatorType::RECURSIVE_CTE:
		return "REC_CTE";
	case LogicalOperatorType::CTE_REF:
		return "CTE_SCAN";
	case LogicalOperatorType::INVALID:
	default:
		return "INVALID";
	}
}

} // namespace duckdb
