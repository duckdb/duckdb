#include "duckdb/function/scalar_function.hpp"

#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"

namespace duckdb {

FunctionLocalState::~FunctionLocalState() {
}

ScalarFunction::ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
                               scalar_function_t function, bind_scalar_function_t bind,
                               dependency_function_t dependency, function_statistics_t statistics,
                               init_local_state_t init_local_state, LogicalType varargs, FunctionStability side_effects,
                               FunctionNullHandling null_handling, bind_lambda_function_t bind_lambda)
    : BaseScalarFunction(std::move(name), std::move(arguments), std::move(return_type), side_effects,
                         std::move(varargs), null_handling),
      function(std::move(function)), bind(bind), init_local_state(init_local_state), dependency(dependency),
      statistics(statistics), bind_lambda(bind_lambda), serialize(nullptr), deserialize(nullptr) {
}

ScalarFunction::ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
                               bind_scalar_function_t bind, dependency_function_t dependency,
                               function_statistics_t statistics, init_local_state_t init_local_state,
                               LogicalType varargs, FunctionStability side_effects, FunctionNullHandling null_handling,
                               bind_lambda_function_t bind_lambda)
    : ScalarFunction(string(), std::move(arguments), std::move(return_type), std::move(function), bind, dependency,
                     statistics, init_local_state, std::move(varargs), side_effects, null_handling, bind_lambda) {
}

bool ScalarFunction::operator==(const ScalarFunction &rhs) const {
	return name == rhs.name && arguments == rhs.arguments && return_type == rhs.return_type && varargs == rhs.varargs &&
	       bind == rhs.bind && dependency == rhs.dependency && statistics == rhs.statistics &&
	       bind_lambda == rhs.bind_lambda;
}

bool ScalarFunction::operator!=(const ScalarFunction &rhs) const {
	return !(*this == rhs);
}

bool ScalarFunction::Equal(const ScalarFunction &rhs) const {
	// number of types
	if (this->arguments.size() != rhs.arguments.size()) {
		return false;
	}
	// argument types
	for (idx_t i = 0; i < this->arguments.size(); ++i) {
		if (this->arguments[i] != rhs.arguments[i]) {
			return false;
		}
	}
	// return type
	if (this->return_type != rhs.return_type) {
		return false;
	}
	// varargs
	if (this->varargs != rhs.varargs) {
		return false;
	}

	return true; // they are equal
}

void ScalarFunction::NopFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() >= 1);
	result.Reference(input.data[0]);
}

template <bool bind_args>
static unique_ptr<FunctionData> FunctionCollationBinder(ClientContext &context, ScalarFunction &bound_function,
                                                        vector<unique_ptr<Expression>> &arguments) {
	LogicalType type = LogicalType::VARCHAR;
	for (auto &arg : arguments) {
		auto &arg_type = arg->return_type;
		// Only works on VARCHAR type and calculate correct collation.
		if (arg_type.id() == LogicalTypeId::VARCHAR) {
			// TryBindComparison will never return false because input types are always VARCHAR.
			BoundComparisonExpression::TryBindComparison(context, type, arg_type, type, ExpressionType::FUNCTION);
		}
	}

	if (bind_args) {
		for (auto &arg : arguments) {
			// PushCollation will return directly if arg's return_type is not VARCHAR type.
			ExpressionBinder::PushCollation(context, arg, type, true);
		}
	}

	if (bound_function.return_type.id() == LogicalTypeId::VARCHAR) {
		// Pass collation to VARCHAR return_type of this function.
		bound_function.return_type = LogicalType::VARCHAR_COLLATION(StringType::GetCollation(type));
	}
	return nullptr;
}

unique_ptr<FunctionData> FunctionRetCollationBinder(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	return FunctionCollationBinder<false>(context, bound_function, arguments);
}

unique_ptr<FunctionData> FunctionAllCollationBinder(ClientContext &context, ScalarFunction &bound_function,
                                                    vector<unique_ptr<Expression>> &arguments) {
	return FunctionCollationBinder<true>(context, bound_function, arguments);
}

} // namespace duckdb
