#include "duckdb/function/scalar/generic_functions.hpp"
#include <iostream>

namespace duckdb {

static void AssertFunction(DataChunk &input, ExpressionState &state, Vector &result) {

	D_ASSERT(input.ColumnCount() == 1 || (input.ColumnCount() == 2
	                                      && input.data[1].GetType().InternalType() == PhysicalType::BOOL));

	if (input.ColumnCount() == 1) {
		// single input: nop
		result.Reference(input.data[0]);
		return;
	}

	// Otherwise, check if we can apply nop to the first column by checking the second input.
	UnifiedVectorFormat condition_vector;
	idx_t count = input.size();
	input.data[1].ToUnifiedFormat(count, condition_vector);
	bool* condition_vector_ptr = (bool*)condition_vector.data;
	for (idx_t i = 0; i < count; i++) {
		auto index = condition_vector.sel->get_index(i);
		if (
		    // If the condition vector does not have a NULL value or not valid, fail the assert.
		    condition_vector.validity.RowIsValid(index)
		    && !condition_vector_ptr[index]) {
			throw InvalidInputException("Assertion failed for " + state.expr.ToString());
		}
	}
	// Just a reference to the existing input.
	result.Reference(input.data[0]);
}

unique_ptr<FunctionData> AssertBindFunction(ClientContext &context, ScalarFunction &bound_function,
                                          vector<unique_ptr<Expression>> &arguments) {
	bound_function.return_type = arguments[0]->return_type;
	return nullptr;
}

void AssertFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet assert_function_set("assert");
	auto assert_fun  = ScalarFunction({LogicalType::ANY, LogicalType::BOOLEAN},
	               LogicalType::ANY,
	               AssertFunction, AssertBindFunction);
	assert_fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	assert_function_set.AddFunction(assert_fun);
	set.AddFunction(assert_function_set);
	assert_function_set.name = "invariant";
	set.AddFunction(assert_function_set);
}

} // namespace duckdb
