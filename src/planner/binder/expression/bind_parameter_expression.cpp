#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ParameterExpression &expr, idx_t depth) {
	D_ASSERT(expr.parameter_nr > 0);
	auto bound_parameter = make_unique<BoundParameterExpression>(expr.parameter_nr);
	bound_parameter->alias = expr.alias;
	if (!binder.parameters) {
		throw BinderException("Unexpected prepared parameter. This type of statement can't be prepared!");
	}
	auto parameter_idx = expr.parameter_nr;
	// check if a parameter value has already been supplied
	if (parameter_idx <= binder.parameters->parameter_data.size()) {
		// it has! emit a constant directly
		auto &data = binder.parameters->parameter_data[parameter_idx - 1];
		auto constant = make_unique<BoundConstantExpression>(data.value);
		constant->alias = expr.alias;
		return BindResult(move(constant));
	}
	auto entry = binder.parameters->parameters.find(parameter_idx);
	if (entry == binder.parameters->parameters.end()) {
		// no entry yet: create a new one
		auto data = make_shared<BoundParameterData>();
		data->return_type = binder.parameters->GetReturnType(parameter_idx - 1);
		bound_parameter->return_type = data->return_type;
		bound_parameter->parameter_data = data;
		binder.parameters->parameters[parameter_idx] = move(data);
	} else {
		// a prepared statement with this parameter index was already there: use it
		auto &data = entry->second;
		bound_parameter->parameter_data = data;
		bound_parameter->return_type = binder.parameters->GetReturnType(parameter_idx - 1);
	}
	return BindResult(move(bound_parameter));
}

} // namespace duckdb
