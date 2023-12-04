#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ParameterExpression &expr, idx_t depth) {
	if (!binder.parameters) {
		throw BinderException("Unexpected prepared parameter. This type of statement can't be prepared!");
	}
	auto parameter_id = expr.identifier;

	D_ASSERT(binder.parameters);
	// Check if a parameter value has already been supplied
	auto &parameter_data = binder.parameters->GetParameterData();
	auto param_data_it = parameter_data.find(parameter_id);
	if (param_data_it != parameter_data.end()) {
		// it has! emit a constant directly
		auto &data = param_data_it->second;
		auto constant = make_uniq<BoundConstantExpression>(data.GetValue());
		constant->alias = expr.alias;
		constant->return_type = binder.parameters->GetReturnType(parameter_id);
		return BindResult(std::move(constant));
	}

	auto bound_parameter = binder.parameters->BindParameterExpression(expr);
	return BindResult(std::move(bound_parameter));
}

} // namespace duckdb
