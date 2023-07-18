#include "duckdb/core_functions/scalar/array_functions.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

static void ArrayFromListFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto array_size = ArrayType::GetSize(result.GetType());

	auto &list_vec = args.data[0];
	list_vec.Flatten(count);
	auto list_data = ListVector::GetData(list_vec);
	auto list_child = ListVector::GetEntry(list_vec);
	auto array_child = ArrayVector::GetEntry(result);

	for (idx_t i = 0; i < count; i++) {
		if (FlatVector::IsNull(list_vec, i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto list_entry = list_data[i];
		if (list_entry.length != array_size) {
			throw Exception("List size must be equal to array size");
		}
		for (idx_t j = 0; j < list_entry.length; j++) {
			auto val = list_child.GetValue(list_entry.offset + j);
			array_child.SetValue((i * array_size) + j, val);
		}
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> ArrayFromListBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw Exception("ArrayFromList expects two arguments");
	}
	auto &size_arg = arguments[1];
	if (!size_arg->IsFoldable()) {
		throw Exception("Array size must be constant");
	}
	if (size_arg->return_type.id() != LogicalTypeId::INTEGER) {
		throw Exception("Array size must be an integer");
	}
	auto size = IntegerValue::Get(ExpressionExecutor::EvaluateScalar(context, *size_arg));
	if (size < 0) {
		throw Exception("Array size must be positive");
	}

	if (arguments[0]->return_type.id() != LogicalTypeId::LIST) {
		throw Exception("First argument must be a list");
	}
	auto child_type = ListType::GetChildType(arguments[0]->return_type);

	bound_function.arguments.push_back(LogicalType::LIST(child_type));
	bound_function.arguments.push_back(LogicalType::INTEGER);
	bound_function.return_type = LogicalType::ARRAY(child_type, size);

	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction ArrayFromListFun::GetFunction() {
	// the arguments and return types are actually set in the binder function
	ScalarFunction fun("array_from_list", {}, LogicalTypeId::ARRAY, ArrayFromListFunction, ArrayFromListBind, nullptr,
	                   nullptr);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
