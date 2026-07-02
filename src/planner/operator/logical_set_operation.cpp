#include "duckdb/planner/operator/logical_set_operation.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {

LogicalSetOperation::LogicalSetOperation(idx_t table_index, idx_t column_count, LogicalOperatorType type,
                                         bool setop_all, bool allow_out_of_order)
    : LogicalOperator(type), table_index(table_index), column_count(column_count), setop_all(setop_all),
      allow_out_of_order(allow_out_of_order) {
}

LogicalSetOperation::LogicalSetOperation(idx_t table_index, idx_t column_count,
                                         vector<unique_ptr<LogicalOperator>> children_p, LogicalOperatorType type,
                                         bool setop_all, bool allow_out_of_order)
    : LogicalOperator(type), table_index(table_index), column_count(column_count), setop_all(setop_all),
      allow_out_of_order(allow_out_of_order) {
	D_ASSERT(type == LogicalOperatorType::LOGICAL_UNION || type == LogicalOperatorType::LOGICAL_EXCEPT ||
	         type == LogicalOperatorType::LOGICAL_INTERSECT);
	children = std::move(children_p);
}

LogicalSetOperation::LogicalSetOperation(idx_t table_index, idx_t column_count, unique_ptr<LogicalOperator> top,
                                         unique_ptr<LogicalOperator> bottom, LogicalOperatorType type, bool setop_all,
                                         bool allow_out_of_order)
    : LogicalOperator(type), table_index(table_index), column_count(column_count), setop_all(setop_all),
      allow_out_of_order(allow_out_of_order) {
	D_ASSERT(type == LogicalOperatorType::LOGICAL_UNION || type == LogicalOperatorType::LOGICAL_EXCEPT ||
	         type == LogicalOperatorType::LOGICAL_INTERSECT);
	children.push_back(std::move(top));
	children.push_back(std::move(bottom));
}

vector<idx_t> LogicalSetOperation::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalSetOperation::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb
