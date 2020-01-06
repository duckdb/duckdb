#include "duckdb/planner/operator/logical_subquery.hpp"

#include "duckdb/planner/table_binding_resolver.hpp"

using namespace duckdb;
using namespace std;

LogicalSubquery::LogicalSubquery(unique_ptr<LogicalOperator> child, index_t table_index)
    : LogicalOperator(LogicalOperatorType::SUBQUERY), table_index(table_index) {
	assert(child);

	TableBindingResolver resolver;
	resolver.VisitOperator(*child);

	for (auto &table : resolver.bound_tables) {
		for(index_t i = 0; i < table.column_count; i++) {
			columns.push_back(ColumnBinding(table.table_index, i));
		}
	}
	children.push_back(move(child));
}
