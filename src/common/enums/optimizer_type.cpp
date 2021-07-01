#include "duckdb/common/enums/optimizer_type.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

string OptimizerTypeToString(OptimizerType type) {
	switch (type) {
	case OptimizerType::EXPRESSION_REWRITER:
		return "expression_rewriter";
	case OptimizerType::FILTER_PULLUP:
		return "filter_pullup";
	case OptimizerType::FILTER_PUSHDOWN:
		return "filter_pushdown";
	case OptimizerType::REGEX_RANGE:
		return "regex_range";
	case OptimizerType::IN_CLAUSE:
		return "in_clause";
	case OptimizerType::JOIN_ORDER:
		return "join_order";
	case OptimizerType::DELIMINATOR:
		return "deliminator";
	case OptimizerType::UNUSED_COLUMNS:
		return "unused_columns";
	case OptimizerType::STATISTICS_PROPAGATION:
		return "statistics_propagation";
	case OptimizerType::COMMON_SUBEXPRESSIONS:
		return "common_subexpressions";
	case OptimizerType::COMMON_AGGREGATE:
		return "common_aggregate";
	case OptimizerType::COLUMN_LIFETIME:
		return "column_lifetime";
	case OptimizerType::TOP_N:
		return "top_n";
	case OptimizerType::REORDER_FILTER:
		return "reorder_filter";
	case OptimizerType::INVALID:
		break;
	}
	return "INVALID";
}

} // namespace duckdb
