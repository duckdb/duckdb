#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar/nested_functions.hpp" // VariableReturnBindData
#include "core_functions/scalar/struct_functions.hpp"

namespace duckdb {

static void StructValuesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto &input = args.data[0];
	const idx_t count = args.size();

	auto &input_children = StructVector::GetEntries(input);
	auto &result_children = StructVector::GetEntries(result);
	D_ASSERT(result_children.size() == input_children.size());

	// UnnamedStruct vector and Struct vector are actually the same underneath, so we can just reference the children
	if (StructType::IsUnnamed(input.GetType())) {
		result.Reference(input);
	} else {
		// We would use result.Reference(input) also for this case,
		// but that function asserts that the logical types are the same
		for (idx_t i = 0; i < input_children.size(); i++) {
			result_children[i]->Reference(*input_children[i]);
		}
	}

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		const bool is_null = ConstantVector::IsNull(input);
		ConstantVector::SetNull(result, is_null);
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);

		// Make result validity to mirror input's nulls
		UnifiedVectorFormat input_data;
		input.ToUnifiedFormat(count, input_data);

		if (!input_data.validity.AllValid()) {
			auto &validity = FlatVector::Validity(result);

			for (idx_t i = 0; i < count; i++) {
				auto idx = input_data.sel->get_index(i);
				if (!input_data.validity.RowIsValid(idx)) {
					validity.SetInvalid(i);
				}
			}
		}
	}
}

// Ensure input is a STRUCT, set return type to an unnamed STRUCT with same child types
static unique_ptr<FunctionData> StructValuesBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	const auto arg_type = arguments[0]->return_type;
	if (arg_type == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	// Since the type of the argument we declared of in `GetFunction` doesn't contain the inner STRUCT type,
	// we should take it from the arguments
	bound_function.arguments[0] = arg_type;

	// Build unnamed children list using only types, with empty names
	child_list_t<LogicalType> unnamed_children;
	auto &children = StructType::GetChildTypes(arguments[0]->return_type);
	unnamed_children.reserve(children.size());
	for (auto &child : children) {
		unnamed_children.emplace_back("", child.second);
	}
	bound_function.SetReturnType(LogicalType::STRUCT(unnamed_children));
	return nullptr;
}

ScalarFunction StructValuesFun::GetFunction() {
	ScalarFunction func({LogicalTypeId::STRUCT}, LogicalTypeId::STRUCT, StructValuesFunction, StructValuesBind);
	return func;
}

} // namespace duckdb
