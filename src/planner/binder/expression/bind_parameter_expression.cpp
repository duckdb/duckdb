#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/main/client_config.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ParameterExpression &expr, idx_t depth) {
	auto parameters = binder.GetParameters();
	auto parameter_id = expr.identifier;

	if (parameters) {
		// Check if a parameter value has already been supplied (named params take precedence)
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
	} else {
		// No explicit parameter value supplied. During PREPARE we leave it as a placeholder slot.
		// During standard binding (direct query or rebind), fall back to a user variable of the same name.
		if (binder.GetBindingMode() != BindingMode::PREPARE) {
			Value variable_value;
			if (ClientConfig::GetConfig(context).GetUserVariable(parameter_id, variable_value)) {
				auto constant = make_uniq<BoundConstantExpression>(variable_value);
				constant->SetAlias(expr.GetAlias());
				return BindResult(std::move(constant));
			}
		}
	}

	auto bound_parameter = parameters->BindParameterExpression(expr);
	return BindResult(std::move(bound_parameter));
}

} // namespace duckdb
