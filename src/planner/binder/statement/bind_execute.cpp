#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ExecuteStatement &stmt) {
	auto parameter_count = stmt.n_param;

	// bind the prepared statement
	auto &client_data = ClientData::Get(context);

	auto entry = client_data.prepared_statements.find(stmt.name);
	if (entry == client_data.prepared_statements.end()) {
		throw BinderException("Prepared statement \"%s\" does not exist", stmt.name);
	}

	// check if we need to rebind the prepared statement
	// this happens if the catalog changes, since in this case e.g. tables we relied on may have been deleted
	auto prepared = entry->second;
	auto &named_param_map = prepared->unbound_statement->named_param_map;

	reference<vector<unique_ptr<ParsedExpression>>> provided_values(stmt.values);
	vector<unique_ptr<ParsedExpression>> mapped_named_values;

	if (named_param_map.size() != stmt.named_values.size()) {
		// Lookup the parameter index from the vector index
		for (idx_t i = 0; i < stmt.values.size(); i++) {
			stmt.named_values[StringUtil::Format("%d", i + 1)] = std::move(stmt.values[i]);
		}
	}

	if (!stmt.named_values.empty()) {
		mapped_named_values = PreparedStatement::PrepareParameters(stmt.named_values, named_param_map);
		provided_values = mapped_named_values;
	}
	// bind any supplied parameters
	vector<Value> bind_values;
	auto constant_binder = Binder::CreateBinder(context);
	constant_binder->SetCanContainNulls(true);
	for (idx_t i = 0; i < provided_values.get().size(); i++) {
		ConstantBinder cbinder(*constant_binder, context, "EXECUTE statement");
		auto bound_expr = cbinder.Bind(provided_values.get()[i]);

		Value value = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
		bind_values.push_back(std::move(value));
	}
	unique_ptr<LogicalOperator> rebound_plan;
	if (prepared->RequireRebind(context, bind_values)) {
		// catalog was modified or statement does not have clear types: rebind the statement before running the execute
		Planner prepared_planner(context);
		for (idx_t i = 0; i < bind_values.size(); i++) {
			prepared_planner.parameter_data.emplace_back(bind_values[i]);
		}
		prepared = prepared_planner.PrepareSQLStatement(entry->second->unbound_statement->Copy());
		rebound_plan = std::move(prepared_planner.plan);
		D_ASSERT(prepared->properties.bound_all_parameters);
		this->bound_tables = prepared_planner.binder->bound_tables;
	}
	// copy the properties of the prepared statement into the planner
	this->properties = prepared->properties;
	this->properties.parameter_count = parameter_count;
	BoundStatement result;
	result.names = prepared->names;
	result.types = prepared->types;

	prepared->Bind(std::move(bind_values));
	if (rebound_plan) {
		auto execute_plan = make_uniq<LogicalExecute>(std::move(prepared));
		execute_plan->children.push_back(std::move(rebound_plan));
		result.plan = std::move(execute_plan);
	} else {
		result.plan = make_uniq<LogicalExecute>(std::move(prepared));
	}
	return result;
}

} // namespace duckdb
