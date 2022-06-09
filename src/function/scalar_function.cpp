#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

FunctionLocalState::~FunctionLocalState() {
}

ScalarFunction::ScalarFunction(string name, vector<LogicalType> arguments, LogicalType return_type,
                               scalar_function_t function, bool has_side_effects, bind_scalar_function_t bind,
                               dependency_function_t dependency, function_statistics_t statistics,
                               init_local_state_t init_local_state, LogicalType varargs, bool propagate_null_values)
    : BaseScalarFunction(move(name), move(arguments), move(return_type), has_side_effects, move(varargs),
                         propagate_null_values),
      function(move(function)), bind(bind), init_local_state(init_local_state), dependency(dependency),
      statistics(statistics) {
}

ScalarFunction::ScalarFunction(vector<LogicalType> arguments, LogicalType return_type, scalar_function_t function,
                               bool propagate_null_values, bool has_side_effects, bind_scalar_function_t bind,
                               dependency_function_t dependency, function_statistics_t statistics,
                               init_local_state_t init_local_state, LogicalType varargs)
    : ScalarFunction(string(), move(arguments), move(return_type), move(function), has_side_effects, bind, dependency,
                     statistics, init_local_state, move(varargs), propagate_null_values) {
}

bool ScalarFunction::operator==(const ScalarFunction &rhs) const {
	return CompareScalarFunctionT(rhs.function) && bind == rhs.bind && dependency == rhs.dependency &&
	       statistics == rhs.statistics;
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

bool ScalarFunction::CompareScalarFunctionT(const scalar_function_t &other) const {
	typedef void(scalar_function_ptr_t)(DataChunk &, ExpressionState &, Vector &);

	auto func_ptr = (scalar_function_ptr_t **)function.template target<scalar_function_ptr_t *>();
	auto other_ptr = (scalar_function_ptr_t **)other.template target<scalar_function_ptr_t *>();

	// Case the functions were created from lambdas the target will return a nullptr
	if (!func_ptr && !other_ptr) {
		return true;
	}
	if (func_ptr == nullptr || other_ptr == nullptr) {
		// scalar_function_t (std::functions) from lambdas cannot be compared
		return false;
	}
	return ((size_t)*func_ptr == (size_t)*other_ptr);
}

void ScalarFunction::NopFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() >= 1);
	result.Reference(input.data[0]);
}

} // namespace duckdb
