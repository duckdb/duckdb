#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void MapUtil::ReinterpretMap(Vector &result, Vector &input, idx_t count) {
	input.Flatten(count);

	auto &input_keys = MapVector::GetKeys(input);
	auto &input_values = MapVector::GetValues(input);

	// Copy the list size
	const auto list_size = ListVector::GetListSize(input);
	ListVector::SetListSize(result, list_size);

	// Copy the list validity
	FlatVector::SetValidity(result, FlatVector::Validity(input));

	// Reference the list data
	FlatVector::SetData(result, FlatVector::GetData(input));
	result.AddHeapReference(input);

	// Copy the struct validity
	auto &result_struct = ListVector::GetEntry(result);
	FlatVector::SetValidity(result_struct, FlatVector::Validity(ListVector::GetEntry(input)));

	// reference the keys / values
	auto &result_keys = MapVector::GetKeys(result);
	result_keys.Reference(input_keys);

	auto &result_values = MapVector::GetValues(result);
	result_values.Reference(input_values);
}

} // namespace duckdb
