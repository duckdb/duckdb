#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ParameterExpression &expr, idx_t depth) {
	D_ASSERT(expr.parameter_nr > 0);
	auto bound_parameter = make_unique<BoundParameterExpression>(expr.parameter_nr);
	if (!binder.parameters) {
		throw std::runtime_error("Unexpected prepared parameter. This type of statement can't be prepared!");
	}
	auto parameter_idx = expr.parameter_nr;
	auto entry = binder.parameters->parameters.find(parameter_idx);
	if (entry == binder.parameters->parameters.end()) {
		// no entry yet: create a new one
		auto data = make_shared<BoundParameterData>();
		data->return_type = binder.parameters->GetReturnType(parameter_idx);
		bound_parameter->return_type = data->return_type;
		bound_parameter->parameter_data = data;
		binder.parameters->parameters[parameter_idx] = move(data);
	} else {
		// a prepared statement with this parameter index was already there: use it
		bound_parameter->parameter_data = entry->second;
		bound_parameter->return_type = entry->second->return_type;
	}
	return BindResult(move(bound_parameter));
}

} // namespace duckdb
