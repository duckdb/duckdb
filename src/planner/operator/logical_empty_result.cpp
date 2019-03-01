#include "planner/operator/logical_empty_result.hpp"
#include "execution/column_binding_resolver.hpp"

using namespace duckdb;
using namespace std;

LogicalEmptyResult::LogicalEmptyResult(unique_ptr<LogicalOperator> op) :
	LogicalOperator(LogicalOperatorType::EMPTY_RESULT) {
	// get the bound tables at this location using a ColumnBindingResolver
	ColumnBindingResolver resolver;
	resolver.VisitOperator(*op);

	this->bound_tables = resolver.GetBoundTables();

	op->ResolveOperatorTypes();
	this->names = op->GetNames();
	this->return_types = op->types;
}
