#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ParameterExpression &expr, idx_t depth) {
	auto parameters = binder.GetParameters();
	if (!parameters) {
		throw BinderException("Unexpected prepared parameter. This type of statement can't be prepared!");
	}
	auto parameter_id = expr.identifier;

	// Check if a parameter value has already been supplied
	auto &parameter_data = parameters->GetParameterData();
	auto param_data_it = parameter_data.find(parameter_id);
	if (param_data_it != parameter_data.end()) {
		// it has! emit a constant directly
		auto &data = param_data_it->second;
		auto return_type = parameters->GetReturnType(parameter_id);
		bool is_literal =
		    return_type.id() == LogicalTypeId::INTEGER_LITERAL || return_type.id() == LogicalTypeId::STRING_LITERAL;
		auto constant = make_uniq<BoundConstantExpression>(data.GetValue());
		constant->SetAlias(expr.GetAlias());
		if (is_literal) {
			return BindResult(std::move(constant));
		}
		auto cast = BoundCastExpression::AddCastToType(context, std::move(constant), return_type);
		return BindResult(std::move(cast));
	}

	auto bound_parameter = parameters->BindParameterExpression(expr);
	return BindResult(std::move(bound_parameter));
}

} // namespace duckdb
