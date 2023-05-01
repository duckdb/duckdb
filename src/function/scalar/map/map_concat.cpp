#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

namespace {

struct MapKeyIndexPair {
	// The index of the map that this key comes from
	idx_t map_index;
	// The index within the maps key_list
	idx_t key_index;
};

} // namespace

static void MapConcatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto count = args.size();
	auto arg_count = args.ColumnCount();

	UnifiedVectorFormat result_data;
	result.ToUnifiedFormat(count, result_data);

	for (idx_t i = 0; i < arg_count; i++) {
	}

	auto &map = args.data[0];
	D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);
	auto child = get_child_vector(map);

	auto &entries = ListVector::GetEntry(result);
	entries.Reference(child);

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	FlatVector::SetData(result, map_data.data);
	FlatVector::SetValidity(result, map_data.validity);
	auto list_size = ListVector::GetListSize(map);
	ListVector::SetListSize(result, list_size);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

static unique_ptr<FunctionData> MapConcatBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {

	auto arg_count = arguments.size();
	if (arg_count < 2) {
		throw InvalidInputException("The provided amount of arguments is incorrect, needs 2 or more");
	}

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		// Prepared statement
		bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}

	// Check and verify that all the maps are of the same type
	for (idx_t i = 1; i < arg_count; i++) {
		auto &arg = arguments[i];
		auto &map = arg->return_type;
		// TODO: add a test using prepared statements
		if (map.id() == LogicalTypeId::UNKNOWN) {
			// Prepared statement
			bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
			bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
			return nullptr;
		}

		auto &expected = arguments[0]->return_type;
		auto &actual = arguments[i]->return_type;

		if (actual != expected) {
			throw InvalidInputException(
			    "'value' type of map differs between arguments, expected '%s', found '%s' instead", expected.ToString(),
			    actual.ToString());
		}
	}

	bound_function.return_type = arguments[0]->return_type;
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

void MapConcatFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map_concat", {}, LogicalTypeId::LIST, MapConcatFunction, MapConcatBind);
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
