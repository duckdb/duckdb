#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/catalog/catalog_entry/prepared_statement_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

BoundStatement Binder::Bind(ExecuteStatement &stmt) {
	BoundStatement result;

	// bind the prepared statement
	auto entry =
	    (PreparedStatementCatalogEntry *)context.prepared_statements->GetEntry(context.ActiveTransaction(), stmt.name);
	if (!entry || entry->deleted) {
		throw BinderException("Could not find prepared statement with that name");
	}
	auto prepared = entry->prepared.get();
	this->read_only = prepared->read_only;
	this->requires_valid_transaction = prepared->requires_valid_transaction;

	vector<Value> bind_values;
	for (idx_t i = 0; i < stmt.values.size(); i++) {
		ConstantBinder binder(*this, context, "EXECUTE statement");
		binder.target_type = prepared->GetType(i + 1);
		auto bound_expr = binder.Bind(stmt.values[i]);

		Value value = ExpressionExecutor::EvaluateScalar(*bound_expr);
		bind_values.push_back(move(value));
	}
	prepared->Bind(move(bind_values));

	result.plan = make_unique<LogicalExecute>(prepared);
	result.names = prepared->names;
	result.types = prepared->sql_types;

	return result;
}
