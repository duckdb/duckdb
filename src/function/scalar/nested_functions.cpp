#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void MapUtil::ReinterpretMap(Vector &result, Vector &input, idx_t count) {
	input.Flatten(count);

	// Copy the list size
	const auto list_size = ListVector::GetListSize(input);
	ListVector::SetListSize(result, list_size);

	// Copy the list validity
	FlatVector::SetValidity(result, FlatVector::Validity(input));

	// Copy the struct validity
	UnifiedVectorFormat input_struct_data;
	ListVector::GetEntry(input).ToUnifiedFormat(list_size, input_struct_data);
	auto &result_struct = ListVector::GetEntry(result);
	FlatVector::SetValidity(result_struct, input_struct_data.validity);

	// Copy the list buffer (the list_entry_t data)
	result.CopyBuffer(input);

	auto &input_keys = MapVector::GetKeys(input);
	auto &result_keys = MapVector::GetKeys(result);
	result_keys.Reference(input_keys);

	auto &input_values = MapVector::GetValues(input);
	auto &result_values = MapVector::GetValues(result);
	result_values.Reference(input_values);

	// Set the right vector type
	result.SetVectorType(input.GetVectorType());
}

} // namespace duckdb
