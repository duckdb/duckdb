#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "core_functions/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct StructKeysBindData : public FunctionData {
	const LogicalType type;
	const bool is_unnamed;
	Vector keys_vector;

	explicit StructKeysBindData(const LogicalType &type_p, bool is_unnamed_p)
	    : type(type_p), is_unnamed(is_unnamed_p), keys_vector(LogicalType::LIST(LogicalType::VARCHAR), 2) {
		// If the struct is unnamed, we don't need to compute or store the keys at all
		if (is_unnamed) {
			return;
		}
		const auto &child_types = StructType::GetChildTypes(type);
		const auto count = child_types.size();

		auto &list_child = ListVector::GetEntry(keys_vector);
		auto child_data = FlatVector::GetData<string_t>(list_child);
		for (idx_t i = 0; i < count; i++) {
			child_data[i] = StringVector::AddString(list_child, child_types[i].first);
		}
		ListVector::SetListSize(keys_vector, count);

		auto list_entries = FlatVector::GetData<list_entry_t>(keys_vector);
		list_entries[0] = {0, count};

		auto &validity = FlatVector::Validity(keys_vector);
		validity.EnsureWritable();
		validity.SetInvalid(1);
	}

	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<StructKeysBindData>();
		// Compare type and flag (content is derived from them)
		return type == o.type && is_unnamed == o.is_unnamed;
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<StructKeysBindData>(type, is_unnamed);
	}
};

static void StructKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	const idx_t count = args.size();

	auto &data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<StructKeysBindData>();
	auto &keys_vector = data.keys_vector;

	// Unnamed STRUCTs should yield NULL (decided at bind time)
	if (data.is_unnamed) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	// If the input is a constant, we must return a CONSTANT_VECTOR
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(input)) {
			ConstantVector::SetNull(result, true);
			return;
		}
		ConstantVector::Reference(result, keys_vector, 0, count);
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
		sel.set_index(i, !is_valid);
	}

	result.Slice(keys_vector, sel, count);
}

static unique_ptr<FunctionData> StructKeysBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments[0]->return_type.id() != LogicalTypeId::STRUCT) {
		throw InvalidInputException("struct_keys() expects a STRUCT argument");
	}

	const bool is_unnamed = StructType::IsUnnamed(arguments[0]->return_type);
	return make_uniq<StructKeysBindData>(arguments[0]->return_type, is_unnamed);
}

ScalarFunction StructKeysFun::GetFunction() {
	ScalarFunction func({LogicalType::ANY}, LogicalType::LIST(LogicalType::VARCHAR), StructKeysFunction);
	func.bind = StructKeysBind;
	return func;
}

} // namespace duckdb
