#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

// Reverse of map_from_entries
static void MapEntriesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	auto &map = args.data[0];
	if (map.GetType().id() == LogicalTypeId::SQLNULL) {
		// Input is a constant NULL
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	MapUtil::ReinterpretMap(result, map, count);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

static LogicalType CreateReturnType(const LogicalType &map) {
	auto &key_type = MapType::KeyType(map);
	auto &value_type = MapType::ValueType(map);

	child_list_t<LogicalType> child_types;
	child_types.push_back(make_pair("key", key_type));
	child_types.push_back(make_pair("value", value_type));

	auto row_type = LogicalType::STRUCT(child_types);
	return LogicalType::LIST(row_type);
}

static unique_ptr<FunctionData> MapEntriesBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
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

	if (map.id() == LogicalTypeId::SQLNULL) {
		// Input is NULL, output is STRUCT(NULL, NULL)[]
		auto map_type = LogicalType::MAP(LogicalTypeId::SQLNULL, LogicalTypeId::SQLNULL);
		bound_function.return_type = CreateReturnType(map_type);
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}

	if (map.id() != LogicalTypeId::MAP) {
		throw InvalidInputException("The provided argument is not a map");
	}
	bound_function.return_type = CreateReturnType(map);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

ScalarFunction MapEntriesFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun({}, LogicalTypeId::LIST, MapEntriesFunction, MapEntriesBind);
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.varargs = LogicalType::ANY;
	return fun;
}

} // namespace duckdb
