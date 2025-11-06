#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "core_functions/scalar/struct_functions.hpp"

namespace duckdb {

static void StructKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	const idx_t count = args.size();

	if (input.GetType().id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("struct_keys() expects a STRUCT argument");
	}

	auto keys = StructVector::GetKeys(input);
	const idx_t key_count = keys.size();
	const auto list_type = LogicalType::LIST(LogicalType::VARCHAR);

	// If the input is a constant during constant folding, we must return a CONSTANT_VECTOR
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(input)) {
			Vector null_vec(list_type);
			ConstantVector::SetNull(null_vec, true);
			result.Reference(null_vec);
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			return;
		}

		Vector list_vec(list_type);
		auto &list_child = ListVector::GetEntry(list_vec);
		list_child.SetVectorType(VectorType::FLAT_VECTOR);
		auto child_data = FlatVector::GetData<string_t>(list_child);
		for (idx_t i = 0; i < key_count; i++) {
			child_data[i] = StringVector::AddString(list_child, keys[i]);
		}
		ListVector::SetListSize(list_vec, key_count);
		auto list_entries = FlatVector::GetData<list_entry_t>(list_vec);
		list_entries[0] = {0, key_count};

		result.Reference(list_vec);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		return;
	}

	// Non-constant input: return a DICTIONARY_VECTOR over two entries (keys list and NULL) to preserve per-row NULLs
	Vector dict_child(list_type);
	auto &list_child = ListVector::GetEntry(dict_child);
	list_child.SetVectorType(VectorType::FLAT_VECTOR);

	auto child_data = FlatVector::GetData<string_t>(list_child);
	for (idx_t i = 0; i < key_count; i++) {
		child_data[i] = StringVector::AddString(list_child, keys[i]);
	}
	ListVector::SetListSize(dict_child, key_count);

	// Set list entries: row 0 spans all key_count child elements; row 1 will be NULL
	auto list_entries = FlatVector::GetData<list_entry_t>(dict_child);
	list_entries[0] = {0, key_count};
	list_entries[1] = {0, 0};

	// Mark row 1 as NULL in the dictionary child
	auto &dict_validity = FlatVector::Validity(dict_child);
	dict_validity.EnsureWritable();
	dict_validity.SetInvalid(1);

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
	return nullptr;
}

ScalarFunction StructKeysFun::GetFunction() {
	ScalarFunction func({LogicalType::ANY}, LogicalType::LIST(LogicalType::VARCHAR), StructKeysFunction);
	func.bind = StructKeysBind;
	return func;
}

} // namespace duckdb
