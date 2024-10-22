#include "duckdb/function/scalar/list/contains_or_position.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/function/scalar/map_functions.hpp"

namespace duckdb {

static void MapContainsFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	const auto count = input.size();

	auto &map_vec = input.data[0];
	auto &key_vec = MapVector::GetKeys(map_vec);
	auto &arg_vec = input.data[1];

	ListSearchOp<false>(map_vec, key_vec, arg_vec, result, count);

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> MapContainsBind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);

	const auto &map = arguments[0]->return_type;
	const auto &key = arguments[1]->return_type;

	if (map.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}

	if (key.id() == LogicalTypeId::UNKNOWN) {
		// Infer the argument type from the map type
		bound_function.arguments[0] = map;
		bound_function.arguments[1] = MapType::KeyType(map);
	} else {
		LogicalType max_child_type;
		if (!LogicalType::TryGetMaxLogicalType(context, MapType::KeyType(map), key, max_child_type)) {
			throw BinderException(
			    "%s: Cannot match element of type '%s' in a map of type '%s' - an explicit cast is required",
			    bound_function.name, key.ToString(), map.ToString());
		}

		bound_function.arguments[0] = LogicalType::MAP(max_child_type, MapType::ValueType(map));
		bound_function.arguments[1] = max_child_type;
	}
	return nullptr;
}

ScalarFunction MapContainsFun::GetFunction() {
	ScalarFunction fun("map_contains", {LogicalType::MAP(LogicalType::ANY, LogicalType::ANY), LogicalType::ANY},
	                   LogicalType::BOOLEAN, MapContainsFunction, MapContainsBind);
	return fun;
}

} // namespace duckdb
