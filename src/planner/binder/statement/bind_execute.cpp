#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

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

	PreparedStatement::VerifyParameters(stmt.named_values, named_param_map);

	auto &mapped_named_values = stmt.named_values;
	// bind any supplied parameters
	case_insensitive_map_t<Value> bind_values;
	auto constant_binder = Binder::CreateBinder(context);
	constant_binder->SetCanContainNulls(true);
	for (auto &pair : mapped_named_values) {
		ConstantBinder cbinder(*constant_binder, context, "EXECUTE statement");
		auto bound_expr = cbinder.Bind(pair.second);

		Value value = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
		bind_values[pair.first] = std::move(value);
	}
	unique_ptr<LogicalOperator> rebound_plan;

	if (prepared->RequireRebind(context, &bind_values)) {
		// catalog was modified or statement does not have clear types: rebind the statement before running the execute
		Planner prepared_planner(context);
		for (auto &pair : bind_values) {
			prepared_planner.parameter_data.emplace(std::make_pair(pair.first, BoundParameterData(pair.second)));
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
