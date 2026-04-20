#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"

namespace duckdb {

void MapUtil::ReinterpretMap(Vector &result, Vector &input, idx_t count) {
	input.Flatten(count);

	auto &input_keys = MapVector::GetKeys(input);
	auto &input_values = MapVector::GetValues(input);

	// Copy the list offsets and top-level validity
	auto result_data = FlatVector::Writer<list_entry_t>(result, count);
	for (auto entry : input.Values<list_entry_t>(count)) {
		if (!entry.IsValid()) {
			result_data.SetInvalid(entry.GetIndex());
			continue;
		}
		result_data[entry.GetIndex()] = entry.GetValue();
	}
	ListVector::SetListSize(result, ListVector::GetListSize(input));

	// Copy the struct validity
	auto &result_struct = ListVector::GetChildMutable(result);
	FlatVector::SetValidity(result_struct, FlatVector::Validity(ListVector::GetChild(input)));

	// reference the keys / values
	auto &result_keys = MapVector::GetKeys(result);
	result_keys.Reference(input_keys);

	auto &result_values = MapVector::GetValues(result);
	result_values.Reference(input_values);
}

} // namespace duckdb
