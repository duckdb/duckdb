#include "duckdb/planner/column_binding.hpp"

#include <utility>

#include "duckdb/planner/expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/to_string.hpp"

namespace duckdb {

ColumnBinding::ColumnBinding() {
}
ColumnBinding::ColumnBinding(TableIndex table, ProjectionIndex column) : table_index(table), column_index(column) {
}

string ColumnBinding::ToString() const {
	return "#[" + to_string(table_index.index) + "." + to_string(column_index) + "]";
}

bool ColumnBinding::operator==(const ColumnBinding &rhs) const {
	return table_index == rhs.table_index && column_index == rhs.column_index;
}

bool ColumnBinding::operator!=(const ColumnBinding &rhs) const {
	return !(*this == rhs);
}
ProjectionIndex ColumnBinding::PushExpression(vector<unique_ptr<Expression>> &expressions,
                                              unique_ptr<Expression> new_expr) {
	if (!new_expr) {
		throw InternalException("No expression to push");
	}
	auto result_idx = expressions.size();
	expressions.push_back(std::move(new_expr));
	return ProjectionIndex(result_idx);
}

} // namespace duckdb
