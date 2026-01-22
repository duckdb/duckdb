#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "core_functions/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct StructKeysBindData : public FunctionData {
	const LogicalType type;
	Vector keys_vector;

	explicit StructKeysBindData(const LogicalType &type_p)
	    : type(type_p), keys_vector(LogicalType::LIST(LogicalType::VARCHAR), 2) {
		const auto &child_types = StructType::GetChildTypes(type);
		const auto count = child_types.size();

		ListVector::Reserve(keys_vector, count);
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
	auto &keys_vector = data.keys_vector;

	// If the input is a constant, we must return a CONSTANT_VECTOR
	if (args.AllConstant()) {
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
	if (is_unnamed) {
		throw InvalidInputException("struct_keys() cannot be applied to an unnamed STRUCT");
	}
	return make_uniq<StructKeysBindData>(arguments[0]->return_type);
}

ScalarFunction StructKeysFun::GetFunction() {
	ScalarFunction func({LogicalType::ANY}, LogicalType::LIST(LogicalType::VARCHAR), StructKeysFunction,
	                    StructKeysBind);
	return func;
}

} // namespace duckdb
