#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

static void MapKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	idx_t count = args.size();

	// Get the arguments vector
	auto &map = args.data[0];
	auto &map_keys = MapVector::GetKeys(map);

	UnifiedVectorFormat map_keys_data;
	UnifiedVectorFormat map_data;
	map_keys.ToUnifiedFormat(count, map_keys_data);
	map.ToUnifiedFormat(count, map_data);

	auto &keys = ListVector::GetEntry(result);

	D_ASSERT(keys.GetType().id() == ListType::GetChildType(map_keys.GetType()).id());

	keys.Reference(ListVector::GetEntry(map_keys));

	// Reference the data for the list_entry_t's
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	FlatVector::SetData(result, map_keys_data.data);
	FlatVector::SetValidity(result, map_data.validity);

	auto list_size = ListVector::GetListSize(map_keys);
	ListVector::SetListSize(result, list_size);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(count);
}

static unique_ptr<FunctionData> MapKeysBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> child_types;

	if (arguments.size() != 1) {
		throw InvalidInputException("The input argument must be a map");
	}
	auto &map = arguments[0]->return_type;

	if (map.id() == LogicalTypeId::UNKNOWN) {
		// Prepared statement
		bound_function.arguments.emplace_back(LogicalTypeId::UNKNOWN);
		bound_function.return_type = LogicalType(LogicalTypeId::SQLNULL);
		return nullptr;
	}

	if (map.id() != LogicalTypeId::MAP) {
		throw InvalidInputException("The provided argument is not a map");
	}

	auto &key_type = MapType::KeyType(map);

	bound_function.return_type = LogicalType::LIST(key_type);
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void MapKeysFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map_keys", {}, LogicalTypeId::LIST, MapKeysFunction, MapKeysBind);
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
