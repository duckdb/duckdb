#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

static void MapKeyValueFunction(DataChunk &args, ExpressionState &state, Vector &result,
                                Vector &(*get_child_vector)(Vector &)) {
	auto &map = args.data[0];

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	if (map.GetType().id() == LogicalTypeId::SQLNULL) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	auto count = args.size();
	D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);
	auto child = get_child_vector(map);

	auto &entries = ListVector::GetEntry(result);
	entries.Reference(child);

	UnifiedVectorFormat map_data;
	map.ToUnifiedFormat(count, map_data);

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	FlatVector::SetData(result, map_data.data);
	FlatVector::SetValidity(result, map_data.validity);
	auto list_size = ListVector::GetListSize(map);
	ListVector::SetListSize(result, list_size);
	if (map.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		result.Slice(*map_data.sel, count);
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

static void MapKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	MapKeyValueFunction(args, state, result, MapVector::GetKeys);
}

static void MapValuesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	MapKeyValueFunction(args, state, result, MapVector::GetValues);
}

static unique_ptr<FunctionData> MapKeyValueBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments,
                                                const LogicalType &(*type_func)(const LogicalType &)) {
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
		// Input is NULL, output is NULL[]
		bound_function.return_type = LogicalType::LIST(LogicalTypeId::SQLNULL);
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}

	if (map.id() != LogicalTypeId::MAP) {
		throw InvalidInputException("The provided argument is not a map");
	}

	auto &type = type_func(map);

	bound_function.return_type = LogicalType::LIST(type);
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static unique_ptr<FunctionData> MapKeysBind(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	return MapKeyValueBind(context, bound_function, arguments, MapType::KeyType);
}

static unique_ptr<FunctionData> MapValuesBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	return MapKeyValueBind(context, bound_function, arguments, MapType::ValueType);
}

ScalarFunction MapKeysFun::GetFunction() {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction function({}, LogicalTypeId::LIST, MapKeysFunction, MapKeysBind);
	function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	BaseScalarFunction::SetReturnsError(function);
	function.varargs = LogicalType::ANY;
	return function;
}

ScalarFunction MapValuesFun::GetFunction() {
	ScalarFunction function({}, LogicalTypeId::LIST, MapValuesFunction, MapValuesBind);
	function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	BaseScalarFunction::SetReturnsError(function);
	function.varargs = LogicalType::ANY;
	return function;
}

} // namespace duckdb
