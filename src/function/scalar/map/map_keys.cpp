#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/map/map_fetch_internals.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

static void MapKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto count = args.size();
	MapFetchInternals<MapKeysFunctor, ListEntryFunctor>(args, state, result);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

static unique_ptr<FunctionData> MapKeysBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	child_list_t<LogicalType> child_types;

	if (arguments.size() != 1) {
		throw InvalidInputException("Too many arguments provided, only expecting a single map");
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
