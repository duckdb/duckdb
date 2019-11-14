#include "duckdb/planner/operator/logical_subquery.hpp"

#include "duckdb/planner/table_binding_resolver.hpp"

using namespace duckdb;
using namespace std;

LogicalSubquery::LogicalSubquery(unique_ptr<LogicalOperator> child, index_t table_index)
    : LogicalOperator(LogicalOperatorType::SUBQUERY), table_index(table_index) {
	assert(child);

	TableBindingResolver resolver;
	resolver.VisitOperator(*child);

	this->bound_tables = resolver.bound_tables;
	this->column_count = 0;
	for (auto &table : bound_tables) {
		column_count += table.column_count;
	}
	children.push_back(move(child));
}
