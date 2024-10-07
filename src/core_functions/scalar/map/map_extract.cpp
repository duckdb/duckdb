#include "duckdb/core_functions/scalar/map_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/bound_expression.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"
namespace duckdb {

static unique_ptr<FunctionData> MapExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("MAP_EXTRACT must have exactly two arguments");
	}

	auto &map_type = arguments[0]->return_type;
	auto &input_type = arguments[1]->return_type;

	if (map_type.id() == LogicalTypeId::SQLNULL) {
		bound_function.return_type = LogicalType::LIST(LogicalTypeId::SQLNULL);
		return make_uniq<VariableReturnBindData>(bound_function.return_type);
	}

	if (map_type.id() != LogicalTypeId::MAP) {
		throw BinderException("MAP_EXTRACT can only operate on MAPs");
	}
	auto &value_type = MapType::ValueType(map_type);

	//! Here we have to construct the List Type that will be returned
	bound_function.return_type = LogicalType::LIST(value_type);
	auto key_type = MapType::KeyType(map_type);
	if (key_type.id() != LogicalTypeId::SQLNULL && input_type.id() != LogicalTypeId::SQLNULL) {
		bound_function.arguments[1] = MapType::KeyType(map_type);
	}
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static void MapExtractFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();

	auto &map_vec = args.data[0];
	auto &arg_vec = args.data[1];

	const auto map_is_null = map_vec.GetType().id() == LogicalTypeId::SQLNULL;
	const auto arg_is_null = arg_vec.GetType().id() == LogicalTypeId::SQLNULL;

	if (map_is_null || arg_is_null) {
		// Short-circuit if either the map or the arg is NULL
		ListVector::SetListSize(result, 0);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::GetData<list_entry_t>(result)[0] = {0, 0};
		result.Verify(count);
		return;
	}

	auto &key_vec = MapVector::GetKeys(map_vec);
	auto &val_vec = MapVector::GetValues(map_vec);

	// Collect the matching positions
	Vector pos_vec(LogicalType::INTEGER, count);
	ListSearchOp<true>(map_vec, key_vec, arg_vec, pos_vec, args.size());

	UnifiedVectorFormat val_format;
	UnifiedVectorFormat pos_format;
	UnifiedVectorFormat lst_format;

	val_vec.ToUnifiedFormat(ListVector::GetListSize(map_vec), val_format);
	pos_vec.ToUnifiedFormat(count, pos_format);
	map_vec.ToUnifiedFormat(count, lst_format);

	const auto pos_data = UnifiedVectorFormat::GetData<int32_t>(pos_format);
	const auto inc_list_data = ListVector::GetData(map_vec);
	const auto out_list_data = ListVector::GetData(result);

	idx_t offset = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto lst_idx = lst_format.sel->get_index(row_idx);
		if (!lst_format.validity.RowIsValid(lst_idx)) {
			FlatVector::SetNull(result, row_idx, true);
			continue;
		}

		auto &inc_list = inc_list_data[lst_idx];
		auto &out_list = out_list_data[row_idx];

		const auto pos_idx = pos_format.sel->get_index(row_idx);
		if (!pos_format.validity.RowIsValid(pos_idx)) {
			// We didnt find the key in the map, so return an empty list
			out_list.offset = offset;
			out_list.length = 0;
			continue;
		}

		// Compute the actual position of the value in the map value vector
		const auto pos = inc_list.offset + UnsafeNumericCast<idx_t>(pos_data[pos_idx] - 1);
		out_list.offset = offset;
		out_list.length = 1;
		ListVector::Append(result, val_vec, pos + 1, pos);
		offset++;
	}

	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(count);
}

ScalarFunction MapExtractFun::GetFunction() {
	ScalarFunction fun({LogicalType::ANY, LogicalType::ANY}, LogicalType::ANY, MapExtractFunc, MapExtractBind);
	fun.varargs = LogicalType::ANY;
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	return fun;
}

} // namespace duckdb
