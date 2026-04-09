#include <memory>
#include <utility>

#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "core_functions/scalar/struct_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
class ClientContext;
struct string_t;

struct StructKeysBindData : public FunctionData {
	const LogicalType type;
	Vector keys_vector;

	explicit StructKeysBindData(const LogicalType &type_p)
	    : type(type_p), keys_vector(LogicalType::LIST(LogicalType::VARCHAR), 2) {
		const auto &child_types = StructType::GetChildTypes(type);
		const auto count = child_types.size();

		auto list_writer = FlatVector::Writer<VectorListType<string_t>>(keys_vector, 2);
		idx_t idx = 0;
		for (auto &child_writer : list_writer.WriteList(count)) {
			child_writer.WriteValue(string_t(child_types[idx++].first));
		}
		list_writer.WriteNull();
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
		ConstantVector::Reference(result, count_t(count), keys_vector, 0, 1);
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
	auto return_type = arguments[0]->GetReturnType();
	if (return_type.id() != LogicalTypeId::STRUCT && !return_type.IsAggregateStateStructType()) {
		throw InvalidInputException("struct_keys() expects a STRUCT argument");
	}

	const bool is_unnamed = StructType::IsUnnamed(arguments[0]->GetReturnType());
	if (is_unnamed) {
		throw InvalidInputException("struct_keys() cannot be applied to an unnamed STRUCT");
	}
	return make_uniq<StructKeysBindData>(arguments[0]->GetReturnType());
}

ScalarFunction StructKeysFun::GetFunction() {
	ScalarFunction func({LogicalTypeId::ANY}, LogicalType::LIST(LogicalType::VARCHAR), StructKeysFunction,
	                    StructKeysBind);
	return func;
}

} // namespace duckdb
