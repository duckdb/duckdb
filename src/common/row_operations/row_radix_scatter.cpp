#include "duckdb/common/helper.hpp"
#include "duckdb/common/radix.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

template <class T>
void TemplatedRadixScatter(UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t add_count,
                           data_ptr_t *key_locations, const bool desc, const bool has_null, const bool nulls_first,
                           const idx_t offset) {
	auto source = UnifiedVectorFormat::GetData<T>(vdata);
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				Radix::EncodeData<T>(key_locations[i] + 1, source[source_idx]);
				// invert bits if desc
				if (desc) {
					for (idx_t s = 1; s < sizeof(T) + 1; s++) {
						*(key_locations[i] + s) = ~*(key_locations[i] + s);
					}
				}
			} else {
				key_locations[i][0] = invalid;
				memset(key_locations[i] + 1, '\0', sizeof(T));
			}
			key_locations[i] += sizeof(T) + 1;
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write value
			Radix::EncodeData<T>(key_locations[i], source[source_idx]);
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < sizeof(T); s++) {
					*(key_locations[i] + s) = ~*(key_locations[i] + s);
				}
			}
			key_locations[i] += sizeof(T);
		}
	}
}

void RadixScatterStringVector(UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t add_count,
                              data_ptr_t *key_locations, const bool desc, const bool has_null, const bool nulls_first,
                              const idx_t prefix_len, idx_t offset) {
	auto source = UnifiedVectorFormat::GetData<string_t>(vdata);
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				Radix::EncodeStringDataPrefix(key_locations[i] + 1, source[source_idx], prefix_len);
				// invert bits if desc
				if (desc) {
					for (idx_t s = 1; s < prefix_len + 1; s++) {
						*(key_locations[i] + s) = ~*(key_locations[i] + s);
					}
				}
			} else {
				key_locations[i][0] = invalid;
				memset(key_locations[i] + 1, '\0', prefix_len);
			}
			key_locations[i] += prefix_len + 1;
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write value
			Radix::EncodeStringDataPrefix(key_locations[i], source[source_idx], prefix_len);
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < prefix_len; s++) {
					*(key_locations[i] + s) = ~*(key_locations[i] + s);
				}
			}
			key_locations[i] += prefix_len;
		}
	}
}

void RadixScatterListVector(Vector &v, UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t add_count,
                            data_ptr_t *key_locations, const bool desc, const bool has_null, const bool nulls_first,
                            const idx_t prefix_len, const idx_t width, const idx_t offset) {
	auto list_data = ListVector::GetData(v);
	auto &child_vector = ListVector::GetEntry(v);
	auto list_size = ListVector::GetListSize(v);
	child_vector.Flatten(list_size);

	// serialize null values
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			data_ptr_t key_location = key_locations[i] + 1;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
				key_locations[i]++;
				auto &list_entry = list_data[source_idx];
				if (list_entry.length > 0) {
					// denote that the list is not empty with a 1
					key_locations[i][0] = 1;
					key_locations[i]++;
					RowOperations::RadixScatter(child_vector, list_size, *FlatVector::IncrementalSelectionVector(), 1,
					                            key_locations + i, false, true, false, prefix_len, width - 1,
					                            list_entry.offset);
				} else {
					// denote that the list is empty with a 0
					key_locations[i][0] = 0;
					key_locations[i]++;
					memset(key_locations[i], '\0', width - 2);
				}
				// invert bits if desc
				if (desc) {
					for (idx_t s = 0; s < width - 1; s++) {
						*(key_location + s) = ~*(key_location + s);
					}
				}
			} else {
				key_locations[i][0] = invalid;
				memset(key_locations[i] + 1, '\0', width - 1);
				key_locations[i] += width;
			}
		}
	} else {
		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			auto &list_entry = list_data[source_idx];
			data_ptr_t key_location = key_locations[i];
			if (list_entry.length > 0) {
				// denote that the list is not empty with a 1
				key_locations[i][0] = 1;
				key_locations[i]++;
				RowOperations::RadixScatter(child_vector, list_size, *FlatVector::IncrementalSelectionVector(), 1,
				                            key_locations + i, false, true, false, prefix_len, width - 1,
				                            list_entry.offset);
			} else {
				// denote that the list is empty with a 0
				key_locations[i][0] = 0;
				key_locations[i]++;
				memset(key_locations[i], '\0', width - 1);
			}
			// invert bits if desc
			if (desc) {
				for (idx_t s = 0; s < width; s++) {
					*(key_location + s) = ~*(key_location + s);
				}
			}
		}
	}
}

void RadixScatterStructVector(Vector &v, UnifiedVectorFormat &vdata, idx_t vcount, const SelectionVector &sel,
                              idx_t add_count, data_ptr_t *key_locations, const bool desc, const bool has_null,
                              const bool nulls_first, const idx_t prefix_len, idx_t width, const idx_t offset) {
	// serialize null values
	if (has_null) {
		auto &validity = vdata.validity;
		const data_t valid = nulls_first ? 1 : 0;
		const data_t invalid = 1 - valid;

		for (idx_t i = 0; i < add_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx) + offset;
			// write validity and according value
			if (validity.RowIsValid(source_idx)) {
				key_locations[i][0] = valid;
			} else {
				key_locations[i][0] = invalid;
			}
			key_locations[i]++;
		}
		width--;
	}
	// serialize the struct
	auto &child_vector = *StructVector::GetEntries(v)[0];
	RowOperations::RadixScatter(child_vector, vcount, *FlatVector::IncrementalSelectionVector(), add_count,
	                            key_locations, false, true, false, prefix_len, width, offset);
	// invert bits if desc
	if (desc) {
		for (idx_t i = 0; i < add_count; i++) {
			for (idx_t s = 0; s < width; s++) {
				*(key_locations[i] - width + s) = ~*(key_locations[i] - width + s);
			}
		}
	}
}

void RowOperations::RadixScatter(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
                                 data_ptr_t *key_locations, bool desc, bool has_null, bool nulls_first,
                                 idx_t prefix_len, idx_t width, idx_t offset) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);
	switch (v.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedRadixScatter<int8_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::INT16:
		TemplatedRadixScatter<int16_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::INT32:
		TemplatedRadixScatter<int32_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::INT64:
		TemplatedRadixScatter<int64_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::UINT8:
		TemplatedRadixScatter<uint8_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::UINT16:
		TemplatedRadixScatter<uint16_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::UINT32:
		TemplatedRadixScatter<uint32_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::UINT64:
		TemplatedRadixScatter<uint64_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::INT128:
		TemplatedRadixScatter<hugeint_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::FLOAT:
		TemplatedRadixScatter<float>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::DOUBLE:
		TemplatedRadixScatter<double>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::INTERVAL:
		TemplatedRadixScatter<interval_t>(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, offset);
		break;
	case PhysicalType::VARCHAR:
		RadixScatterStringVector(vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, prefix_len, offset);
		break;
	case PhysicalType::LIST:
		RadixScatterListVector(v, vdata, sel, ser_count, key_locations, desc, has_null, nulls_first, prefix_len, width,
		                       offset);
		break;
	case PhysicalType::STRUCT:
		RadixScatterStructVector(v, vdata, vcount, sel, ser_count, key_locations, desc, has_null, nulls_first,
		                         prefix_len, width, offset);
		break;
	default:
		throw NotImplementedException("Cannot ORDER BY column with type %s", v.GetType().ToString());
	}
}

} // namespace duckdb
