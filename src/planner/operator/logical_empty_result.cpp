#include "duckdb/planner/operator/logical_empty_result.hpp"

#include "duckdb/planner/table_binding_resolver.hpp"

using namespace duckdb;
using namespace std;

LogicalEmptyResult::LogicalEmptyResult(unique_ptr<LogicalOperator> op)
    : LogicalOperator(LogicalOperatorType::EMPTY_RESULT) {
	TableBindingResolver resolver;
	resolver.VisitOperator(*op);

	this->bound_tables = resolver.bound_tables;

	op->ResolveOperatorTypes();
	this->return_types = op->types;
}
