#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ParameterExpression &expr, idx_t depth) {
	auto bound_parameter = make_uniq<BoundParameterExpression>(expr.identifier);
	bound_parameter->alias = expr.alias;
	if (!binder.parameters) {
		throw BinderException("Unexpected prepared parameter. This type of statement can't be prepared!");
	}
	auto parameter_id = expr.identifier;
	// check if a parameter value has already been supplied
	if (binder.parameters->parameter_data.count(parameter_id)) {
		// it has! emit a constant directly
		auto &data = binder.parameters->parameter_data[parameter_id];
		auto constant = make_uniq<BoundConstantExpression>(data.GetValue());
		constant->alias = expr.alias;
		return BindResult(std::move(constant));
	}

	auto entry = binder.parameters->parameters.find(parameter_id);
	if (entry == binder.parameters->parameters.end()) {
		// no entry yet: create a new one
		auto data = make_shared<BoundParameterData>();
		data->return_type = binder.parameters->GetReturnType(parameter_id);
		bound_parameter->return_type = data->return_type;
		bound_parameter->parameter_data = data;
		binder.parameters->parameters[parameter_id] = std::move(data);
	} else {
		// a prepared statement with this parameter index was already there: use it
		auto &data = entry->second;
		bound_parameter->parameter_data = data;
		bound_parameter->return_type = binder.parameters->GetReturnType(parameter_id);
	}
	return BindResult(std::move(bound_parameter));
}

} // namespace duckdb
