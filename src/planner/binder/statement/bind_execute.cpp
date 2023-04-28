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

	case_insensitive_map_t<reference<unique_ptr<ParsedExpression>>> intermediate;
	for (auto &pair : stmt.named_values) {
		intermediate.emplace(pair);
	}

	// Add the unnamed values to the intermediate if they are present
	if (named_param_map.size() != stmt.named_values.size()) {
		// Lookup the parameter index from the vector index
		for (idx_t i = 0; i < stmt.values.size(); i++) {
			intermediate.emplace(std::make_pair(StringUtil::Format("%d", i + 1),
			                                    reference<unique_ptr<ParsedExpression>>(stmt.values[i])));
		}
	}

	auto mapped_named_values = PreparedStatement::PrepareParameters(stmt.values, intermediate, named_param_map);

	// bind any supplied parameters
	vector<Value> bind_values;
	auto constant_binder = Binder::CreateBinder(context);
	constant_binder->SetCanContainNulls(true);
	for (idx_t i = 0; i < mapped_named_values.size(); i++) {
		ConstantBinder cbinder(*constant_binder, context, "EXECUTE statement");
		auto bound_expr = cbinder.Bind(mapped_named_values[i]);

		Value value = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
		bind_values.push_back(std::move(value));
	}
	unique_ptr<LogicalOperator> rebound_plan;
	vector<reference<Value>> bind_value_references;
	for (auto &val : bind_values) {
		bind_value_references.push_back(val);
	}
	if (prepared->RequireRebind(context, bind_value_references)) {
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
