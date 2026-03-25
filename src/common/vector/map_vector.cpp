#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/value_map.hpp"

namespace duckdb {

Vector &MapVector::GetKeys(Vector &vector) {
	auto &entries = StructVector::GetEntries(ListVector::GetEntry(vector));
	D_ASSERT(entries.size() == 2);
	return entries[0];
}
Vector &MapVector::GetValues(Vector &vector) {
	auto &entries = StructVector::GetEntries(ListVector::GetEntry(vector));
	D_ASSERT(entries.size() == 2);
	return entries[1];
}

const Vector &MapVector::GetKeys(const Vector &vector) {
	return GetKeys((Vector &)vector);
}
const Vector &MapVector::GetValues(const Vector &vector) {
	return GetValues((Vector &)vector);
}

MapInvalidReason MapVector::CheckMapValidity(Vector &map, idx_t count, const SelectionVector &sel) {
	D_ASSERT(map.GetType().id() == LogicalTypeId::MAP);

	// unify the MAP vector, which is a physical LIST vector
	UnifiedVectorFormat map_data;
	map.ToUnifiedFormat(count, map_data);
	auto map_entries = UnifiedVectorFormat::GetDataNoConst<list_entry_t>(map_data);
	auto maps_length = ListVector::GetListSize(map);

	// unify the child vector containing the keys
	auto &keys = MapVector::GetKeys(map);
	UnifiedVectorFormat key_data;
	keys.ToUnifiedFormat(maps_length, key_data);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto mapped_row = sel.get_index(row_idx);
		auto map_idx = map_data.sel->get_index(mapped_row);

		if (!map_data.validity.RowIsValid(map_idx)) {
			continue;
		}

		value_set_t unique_keys;
		auto length = map_entries[map_idx].length;
		auto offset = map_entries[map_idx].offset;

		for (idx_t child_idx = 0; child_idx < length; child_idx++) {
			auto key_idx = key_data.sel->get_index(offset + child_idx);

			if (!key_data.validity.RowIsValid(key_idx)) {
				return MapInvalidReason::NULL_KEY;
			}

			auto value = keys.GetValue(key_idx);
			auto unique = unique_keys.insert(value).second;
			if (!unique) {
				return MapInvalidReason::DUPLICATE_KEY;
			}
		}
	}

	return MapInvalidReason::VALID;
}

void MapVector::MapConversionVerify(Vector &vector, idx_t count) {
	auto reason = MapVector::CheckMapValidity(vector, count);
	EvalMapInvalidReason(reason);
}

void MapVector::EvalMapInvalidReason(MapInvalidReason reason) {
	switch (reason) {
	case MapInvalidReason::VALID:
		return;
	case MapInvalidReason::DUPLICATE_KEY:
		throw InvalidInputException("Map keys must be unique.");
	case MapInvalidReason::NULL_KEY:
		throw InvalidInputException("Map keys can not be NULL.");
	case MapInvalidReason::NOT_ALIGNED:
		throw InvalidInputException("The map key list does not align with the map value list.");
	case MapInvalidReason::INVALID_PARAMS:
		throw InvalidInputException("Invalid map argument(s). Valid map arguments are a list of key-value pairs (MAP "
		                            "{'key1': 'val1', ...}), two lists (MAP ([1, 2], [10, 11])), or no arguments.");
	default:
		throw InternalException("MapInvalidReason not implemented");
	}
}

} // namespace duckdb
