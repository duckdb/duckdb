#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

// Reverse of map_from_values
static void MapValuesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	idx_t count = args.size();

	// Get the arguments vector
	auto &map = args.data[0];
	auto &map_values = MapVector::GetValues(map);

	UnifiedVectorFormat map_values_data;
	UnifiedVectorFormat map_data;
	map_values.ToUnifiedFormat(count, map_values_data);
	map.ToUnifiedFormat(count, map_data);

	auto &values = ListVector::GetEntry(result);

	D_ASSERT(values.GetType().id() == ListType::GetChildType(map_values.GetType()).id());

	// Reference the map's keys and values, so we can do a zero-copy
	// This might be a problem if the maps values get altered in this query, but they *should* be immutable
	// Or if the resulting struct vectors get altered, it could have an effect on the projection results of the map
	values.Reference(ListVector::GetEntry(map_values));

	// Reference the data for the list_entry_t's
	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	FlatVector::SetData(result, map_values_data.data);
	FlatVector::SetValidity(result, map_data.validity);

	auto list_size = ListVector::GetListSize(map_values);
	ListVector::SetListSize(result, list_size);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(count);
}

static unique_ptr<FunctionData> MapValuesBind(ClientContext &context, ScalarFunction &bound_function,
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

	auto &value_type = MapType::ValueType(map);

	bound_function.return_type = LogicalType::LIST(value_type);
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

void MapValuesFun::RegisterFunction(BuiltinFunctions &set) {
	//! the arguments and return types are actually set in the binder function
	ScalarFunction fun("map_values", {}, LogicalTypeId::LIST, MapValuesFunction, MapValuesBind);
	fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	fun.varargs = LogicalType::ANY;
	set.AddFunction(fun);
}

} // namespace duckdb
