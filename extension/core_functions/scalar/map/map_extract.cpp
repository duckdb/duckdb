#include "core_functions/scalar/map_functions.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar/list/contains_or_position.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

static void MapExtractValueFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();

	auto &map_vec = args.data[0];
	auto &arg_vec = args.data[1];

	auto &key_vec = MapVector::GetKeys(map_vec);
	auto &val_vec = MapVector::GetValues(map_vec);

	// Collect the matching positions
	Vector pos_vec(LogicalType::INTEGER, count);
	ListSearchOp<int32_t>(map_vec, key_vec, arg_vec, pos_vec, args.size());

	UnifiedVectorFormat pos_format;
	UnifiedVectorFormat lst_format;

	pos_vec.ToUnifiedFormat(count, pos_format);
	map_vec.ToUnifiedFormat(count, lst_format);

	const auto pos_data = UnifiedVectorFormat::GetData<int32_t>(pos_format);
	const auto inc_list_data = ListVector::GetData(map_vec);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto lst_idx = lst_format.sel->get_index(row_idx);
		if (!lst_format.validity.RowIsValid(lst_idx)) {
			FlatVector::SetNull(result, row_idx, true);
			continue;
		}

		const auto pos_idx = pos_format.sel->get_index(row_idx);
		if (!pos_format.validity.RowIsValid(pos_idx)) {
			// We didnt find the key in the map, so return NULL
			FlatVector::SetNull(result, row_idx, true);
			continue;
		}

		// Compute the actual position of the value in the map value vector
		const auto pos = inc_list_data[lst_idx].offset + UnsafeNumericCast<idx_t>(pos_data[pos_idx] - 1);
		VectorOperations::Copy(val_vec, result, pos + 1, pos, row_idx);
	}

	if (args.size() == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(count);
}

static void MapExtractListFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();

	auto &map_vec = args.data[0];
	auto &arg_vec = args.data[1];

	auto &key_vec = MapVector::GetKeys(map_vec);
	auto &val_vec = MapVector::GetValues(map_vec);

	// Collect the matching positions
	Vector pos_vec(LogicalType::INTEGER, count);
	ListSearchOp<int32_t>(map_vec, key_vec, arg_vec, pos_vec, args.size());

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
		const auto lst_idx = lst_format.sel->get_index(row_idx);
		if (!lst_format.validity.RowIsValid(lst_idx)) {
			FlatVector::SetNull(result, row_idx, true);
			continue;
		}

		auto &inc_list = inc_list_data[lst_idx];
		auto &out_list = out_list_data[row_idx];

		const auto pos_idx = pos_format.sel->get_index(row_idx);
		if (!pos_format.validity.RowIsValid(pos_idx)) {
			// We didnt find the key in the map, so return empty list
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

ScalarFunction MapExtractValueFun::GetFunction() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");

	ScalarFunction fun({LogicalType::MAP(key_type, val_type), key_type}, val_type, MapExtractValueFunc);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

ScalarFunction MapExtractFun::GetFunction() {
	auto key_type = LogicalType::TEMPLATE("K");
	auto val_type = LogicalType::TEMPLATE("V");

	ScalarFunction fun({LogicalType::MAP(key_type, val_type), key_type}, LogicalType::LIST(val_type),
	                   MapExtractListFunc);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return fun;
}

} // namespace duckdb
