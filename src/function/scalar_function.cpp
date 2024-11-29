#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

FunctionLocalState::~FunctionLocalState() {
}

ScalarFunctionInfo::~ScalarFunctionInfo() {
}

ScalarFunction::ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
                               scalar_function_t function, bind_scalar_function_t bind,
                               bind_scalar_function_extended_t bind_extended, function_statistics_t statistics,
                               init_local_state_t init_local_state, LogicalType varargs, FunctionStability side_effects,
                               FunctionNullHandling null_handling, bind_lambda_function_t bind_lambda)
    : BaseScalarFunction(std::move(name), std::move(arguments), std::move(return_type), side_effects,
                         std::move(varargs), null_handling),
      function(std::move(function)), bind(bind), bind_extended(bind_extended), init_local_state(init_local_state),
      statistics(statistics), bind_lambda(bind_lambda), bind_expression(nullptr), get_modified_databases(nullptr),
      serialize(nullptr), deserialize(nullptr) {
}

ScalarFunction::ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
                               bind_scalar_function_t bind, bind_scalar_function_extended_t bind_extended,
                               function_statistics_t statistics, init_local_state_t init_local_state,
                               LogicalType varargs, FunctionStability side_effects, FunctionNullHandling null_handling,
                               bind_lambda_function_t bind_lambda)
    : ScalarFunction(string(), std::move(arguments), std::move(return_type), std::move(function), bind, bind_extended,
                     statistics, init_local_state, std::move(varargs), side_effects, null_handling, bind_lambda) {
}

bool ScalarFunction::operator==(const ScalarFunction &rhs) const {
	return name == rhs.name && arguments == rhs.arguments && return_type == rhs.return_type && varargs == rhs.varargs &&
	       bind == rhs.bind && bind_extended == rhs.bind_extended && statistics == rhs.statistics &&
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

} // namespace duckdb
