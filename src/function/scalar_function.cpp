#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

bool ScalarFunctionCallbacks::operator==(const ScalarFunctionCallbacks &rhs) const {
	return bind == rhs.bind && init_local_state == rhs.init_local_state && statistics == rhs.statistics &&
	       bind_lambda == rhs.bind_lambda && bind_expression == rhs.bind_expression &&
	       get_modified_databases == rhs.get_modified_databases && serialize == rhs.serialize &&
	       deserialize == rhs.deserialize && filter_prune == rhs.filter_prune;
}

bool ScalarFunctionCallbacks::operator!=(const ScalarFunctionCallbacks &rhs) const {
	return !(*this == rhs);
}

FunctionLocalState::~FunctionLocalState() {
}

ScalarFunctionInfo::~ScalarFunctionInfo() {
}

ScalarFunction::ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
                               scalar_function_t function, bind_scalar_function_t bind,
                               function_statistics_t statistics, init_local_state_t init_local_state,
                               LogicalType varargs, FunctionStability side_effects, FunctionNullHandling null_handling,
                               bind_lambda_function_t bind_lambda)
    : SimpleFunction(std::move(name), std::move(arguments), std::move(return_type), std::move(varargs)) {
	properties.stability = side_effects;
	properties.null_handling = null_handling;

	callbacks.function = std::move(function);
	callbacks.bind = bind;
	callbacks.init_local_state = init_local_state;
	callbacks.statistics = statistics;
	callbacks.bind_lambda = bind_lambda;
}

ScalarFunction::ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
                               bind_scalar_function_t bind, function_statistics_t statistics,
                               init_local_state_t init_local_state, LogicalType varargs, FunctionStability side_effects,
                               FunctionNullHandling null_handling, bind_lambda_function_t bind_lambda)
    : ScalarFunction(string(), std::move(arguments), std::move(return_type), std::move(function), bind, statistics,
                     init_local_state, std::move(varargs), side_effects, null_handling, bind_lambda) {
}

bool ScalarFunction::operator==(const ScalarFunction &rhs) const {
	return name == rhs.name && arguments == rhs.GetArguments() && return_type == rhs.return_type &&
	       varargs == rhs.GetVarArgs() && callbacks == rhs.callbacks && properties == rhs.properties;
}

bool ScalarFunction::operator!=(const ScalarFunction &rhs) const {
	return !(*this == rhs);
}

bool ScalarFunction::Equal(const ScalarFunction &rhs) const {
	// number of types
	if (this->GetArguments().size() != rhs.GetArguments().size()) {
		return false;
	}
	// argument types
	for (idx_t i = 0; i < this->GetArguments().size(); ++i) {
		if (this->GetArguments()[i] != rhs.GetArguments()[i]) {
			return false;
		}
	}
	// return type
	if (this->return_type != rhs.return_type) {
		return false;
	}
	// varargs
	if (this->GetVarArgs() != rhs.GetVarArgs()) {
		return false;
	}

	return true; // they are equal
}

void ScalarFunction::NopFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() >= 1);
	result.Reference(input.data[0]);
}

unique_ptr<BoundFunctionExpression>
ScalarFunction::Bind(ClientContext &context, vector<unique_ptr<Expression>> arguments, optional_ptr<Binder> binder) {
	FunctionBinder func_binder(context);
	auto expr = func_binder.BindScalarFunction(*this, std::move(arguments), binder);

	if (expr->GetExpressionType() != ExpressionType::BOUND_FUNCTION) {
		throw InvalidInputException("BindScalarFunction did not return a BoundFunctionExpression");
	}

	return unique_ptr_cast<Expression, BoundFunctionExpression>(std::move(expr));
}
} // namespace duckdb
