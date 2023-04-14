#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/execute_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_execute.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

static string ExcessValuesException(const case_insensitive_map_t<idx_t> &parameters,
                                    case_insensitive_map_t<unique_ptr<ParsedExpression>> &values) {
	// Too many values
	vector<string> excess_values;
	for (auto &pair : values) {
		auto &name = pair.first;
		if (!parameters.count(name)) {
			excess_values.push_back(name);
		}
	}
	return StringUtil::Format("Some of the provided named values don't have a matching parameter: %s",
	                          StringUtil::Join(excess_values, ", "));
}

static string MissingValuesException(const case_insensitive_map_t<idx_t> &parameters,
                                     case_insensitive_map_t<unique_ptr<ParsedExpression>> &values) {
	// Missing values
	vector<string> missing_values;
	for (auto &pair : parameters) {
		auto &name = pair.first;
		if (!values.count(name)) {
			missing_values.push_back(name);
		}
	}
	return StringUtil::Format("Values were not provided for the following prepared statement parameters: %s",
	                          StringUtil::Join(missing_values, ", "));
}

vector<unique_ptr<ParsedExpression>> PrepareParameters(ExecuteStatement &stmt,
                                                       const case_insensitive_map_t<idx_t> &named_params) {
	if (named_params.empty()) {
		if (!stmt.named_values.empty()) {
			// None of the parameters are named, but the execute statement does have named values
			throw InvalidInputException("The prepared statement doesn't expect any named parameters, but the execute "
			                            "statement does contain name = value pairs");
		}
		return std::move(stmt.values);
	}
	if (stmt.named_values.empty()) {
		throw InvalidInputException("The prepared statement expects named parameters, but none were provided");
	}
	if (named_params.size() != stmt.named_values.size()) {
		// Mismatch in expected and provided parameters/values
		if (named_params.size() > stmt.named_values.size()) {
			throw InvalidInputException(MissingValuesException(named_params, stmt.named_values));
		} else {
			D_ASSERT(stmt.named_values.size() > named_params.size());
			throw InvalidInputException(ExcessValuesException(named_params, stmt.named_values));
		}
	}
	vector<unique_ptr<ParsedExpression>> result(named_params.size());
	for (auto &pair : named_params) {
		auto &name = pair.first;
		auto entry = stmt.named_values.find(name);
		if (entry == stmt.named_values.end()) {
			throw InvalidInputException("Expected a parameter '%s' was not found in the provided values", name);
		}
		auto &named_value = entry->second;

		auto &param_idx = pair.second;
		D_ASSERT(param_idx > 0);
		D_ASSERT(param_idx - 1 < result.size());
		result[param_idx - 1] = std::move(named_value);
	}
#ifdef DEBUG
	for (idx_t i = 0; i < result.size(); i++) {
		D_ASSERT(result[i] != nullptr);
	}
#endif
	return result;
}

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

	auto provided_values = PrepareParameters(stmt, named_param_map);
	// bind any supplied parameters
	vector<Value> bind_values;
	auto constant_binder = Binder::CreateBinder(context);
	constant_binder->SetCanContainNulls(true);
	for (idx_t i = 0; i < provided_values.size(); i++) {
		ConstantBinder cbinder(*constant_binder, context, "EXECUTE statement");
		auto bound_expr = cbinder.Bind(provided_values[i]);

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
