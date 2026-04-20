#include "duckdb/common/vector/list_vector.hpp"
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
		auto child_data = FlatVector::Writer<string_t>(list_child, count);
		for (idx_t i = 0; i < count; i++) {
			child_data.PushValue(string_t(child_types[i].first));
		}
		ListVector::SetListSize(keys_vector, count);

		auto list_entries = FlatVector::Writer<list_entry_t>(keys_vector, 2);
		list_entries.PushValue(list_entry_t(0, count));
		list_entries.PushInvalid();
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
		ConstantVector::Reference(result, keys_vector, 0, count);
		return;
	}

	// Non-constant input: return a DICTIONARY_VECTOR over two entries (keys list and NULL) to preserve per-row NULLs
	// Build the dictionary selection: 0 for non-null input, 1 for null input
	SelectionVector sel(count);
	auto validity_entries = input.Validity(count);
	for (idx_t i = 0; i < count; i++) {
		sel.set_index(i, !validity_entries.IsValid(i));
	}

	result.Slice(keys_vector, sel, count);
}

static unique_ptr<FunctionData> StructKeysBind(BindScalarFunctionInput &input) {
	auto &arguments = input.GetArguments();
	auto return_type = arguments[0]->return_type;
	if (return_type.id() != LogicalTypeId::STRUCT && !return_type.IsAggregateStateStructType()) {
		throw InvalidInputException("struct_keys() expects a STRUCT argument");
	}

	const bool is_unnamed = StructType::IsUnnamed(arguments[0]->return_type);
	if (is_unnamed) {
		throw InvalidInputException("struct_keys() cannot be applied to an unnamed STRUCT");
	}
	return make_uniq<StructKeysBindData>(arguments[0]->return_type);
}

ScalarFunction StructKeysFun::GetFunction() {
	ScalarFunction func({LogicalTypeId::ANY}, LogicalType::LIST(LogicalType::VARCHAR), StructKeysFunction,
	                    StructKeysBind);
	return func;
}

} // namespace duckdb
