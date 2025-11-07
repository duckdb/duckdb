#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "core_functions/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct StructKeysBindData : public FunctionData {
	LogicalType type;
	Vector dict_child;

	explicit StructKeysBindData(LogicalType type_p) : type(std::move(type_p)), dict_child(LogicalType::LIST(LogicalType::VARCHAR)) {
		// TODO - Can I set capacity of dict_child according to constant vector / flat vector?
		const auto &child_types = StructType::GetChildTypes(type);
		const auto count = child_types.size();

		auto &list_child = ListVector::GetEntry(dict_child);
		list_child.SetVectorType(VectorType::FLAT_VECTOR);
		auto child_data = FlatVector::GetData<string_t>(list_child);
		for (idx_t i = 0; i < count; i++) {
			child_data[i] = StringVector::AddString(list_child, child_types[i].first);
		}
		ListVector::SetListSize(dict_child, count);

		auto list_entries = FlatVector::GetData<list_entry_t>(dict_child);
		list_entries[0] = {0, count};
		list_entries[1] = {0, 0};

		auto &dict_validity = FlatVector::Validity(dict_child);
		dict_validity.EnsureWritable();
		dict_validity.SetInvalid(1);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<StructKeysBindData>();
		// It should be fine to compare just the type as the content is derived from it
		return type == o.type;
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StructKeysBindData>(type);
	}
};

static void StructKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	const idx_t count = args.size();

	auto &data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<StructKeysBindData>();
	auto &dict_child = data.dict_child;


	// If the input is a constant during constant folding, we must return a CONSTANT_VECTOR
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(input)) {
			Vector null_vec(LogicalType::LIST(LogicalType::VARCHAR));
			ConstantVector::SetNull(null_vec, true);
			result.Reference(null_vec);
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			return;
		}

		result.Reference(dict_child);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	}

	// Non-constant input: return a DICTIONARY_VECTOR over two entries (keys list and NULL) to preserve per-row NULLs
	// Build the dictionary selection: 0 for non-null input, 1 for null input
	SelectionVector sel(count);
	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		const bool is_valid = input_data.validity.RowIsValid(idx);
		sel.set_index(i, is_valid ? 0 : 1);
	}

	result.Reference(dict_child);
	result.Dictionary(2, sel, count);
}

static unique_ptr<FunctionData> StructKeysBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("struct_keys() expects a STRUCT argument");
	}

	return make_uniq<StructKeysBindData>(arguments[0]->return_type);
}

ScalarFunction StructKeysFun::GetFunction() {
	ScalarFunction func({LogicalType::ANY}, LogicalType::LIST(LogicalType::VARCHAR), StructKeysFunction);
	func.bind = StructKeysBind;
	return func;
}

} // namespace duckdb
