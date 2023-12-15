#include "duckdb/core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

// Reverse of map_from_entries
static void MapEntriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	idx_t count = args.size();

	result.Reinterpret(args.data[0]);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

static unique_ptr<FunctionData> MapEntriesBind(ClientContext &context, ScalarFunction &bound_function,
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
	auto &value_type = MapType::ValueType(map);

	child_types.push_back(make_pair("key", key_type));
	child_types.push_back(make_pair("value", value_type));

	auto row_type = LogicalType::STRUCT(child_types);

	bound_function.return_type = LogicalType::LIST(row_type);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction MapEntriesFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun({}, LogicalTypeId::LIST, MapEntriesFunction, MapEntriesBind);
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	fun.varargs = LogicalType::ANY;
	return fun;
}

} // namespace duckdb
