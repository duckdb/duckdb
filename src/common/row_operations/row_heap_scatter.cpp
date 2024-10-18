#include "duckdb/common/helper.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/uhugeint.hpp"

namespace duckdb {

using ValidityBytes = TemplatedValidityMask<uint8_t>;

NestedValidity::NestedValidity(data_ptr_t validitymask_location)
    : list_validity_location(validitymask_location), struct_validity_locations(nullptr), entry_idx(0), idx_in_entry(0),
      list_validity_offset(0) {
}

NestedValidity::NestedValidity(data_ptr_t *validitymask_locations, idx_t child_vector_index)
    : list_validity_location(nullptr), struct_validity_locations(validitymask_locations), entry_idx(0), idx_in_entry(0),
      list_validity_offset(0) {
	ValidityBytes::GetEntryIndex(child_vector_index, entry_idx, idx_in_entry);
}

void NestedValidity::SetInvalid(idx_t idx) {
	if (list_validity_location) {
		// Is List

		idx = idx + list_validity_offset;

		idx_t list_entry_idx;
		idx_t list_idx_in_entry;
		ValidityBytes::GetEntryIndex(idx, list_entry_idx, list_idx_in_entry);
		const auto bit = ~(1UL << list_idx_in_entry);
		list_validity_location[list_entry_idx] &= bit;
	} else {
		// Is Struct
		const auto bit = ~(1UL << idx_in_entry);
		*(struct_validity_locations[idx] + entry_idx) &= bit;
	}
}

void NestedValidity::OffsetListBy(idx_t offset) {
	list_validity_offset += offset;
}

bool NestedValidity::IsValid(idx_t idx) {
	if (list_validity_location) {
		// Is List

		idx = idx + list_validity_offset;

		idx_t list_entry_idx;
		idx_t list_idx_in_entry;
		ValidityBytes::GetEntryIndex(idx, list_entry_idx, list_idx_in_entry);
		const auto bit = (1UL << list_idx_in_entry);
		return list_validity_location[list_entry_idx] & bit;
	} else {
		// Is Struct
		const auto bit = (1UL << idx_in_entry);
		return *(struct_validity_locations[idx] + entry_idx) & bit;
	}
}

static void ComputeStringEntrySizes(UnifiedVectorFormat &vdata, idx_t entry_sizes[], const idx_t ser_count,
                                    const SelectionVector &sel, const idx_t offset) {
	auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < ser_count; i++) {
		auto idx = sel.get_index(i);
		auto str_idx = vdata.sel->get_index(idx + offset);
		if (vdata.validity.RowIsValid(str_idx)) {
			entry_sizes[i] += sizeof(uint32_t) + strings[str_idx].GetSize();
		}
	}
}

static void ComputeStructEntrySizes(Vector &v, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
                                    const SelectionVector &sel, idx_t offset) {
	// obtain child vectors
	idx_t num_children;
	auto &children = StructVector::GetEntries(v);
	num_children = children.size();
	// add struct validitymask size
	const idx_t struct_validitymask_size = (num_children + 7) / 8;
	for (idx_t i = 0; i < ser_count; i++) {
		entry_sizes[i] += struct_validitymask_size;
	}
	// compute size of child vectors
	for (auto &struct_vector : children) {
		RowOperations::ComputeEntrySizes(*struct_vector, entry_sizes, vcount, ser_count, sel, offset);
	}
}

static void ComputeListEntrySizes(Vector &v, UnifiedVectorFormat &vdata, idx_t entry_sizes[], idx_t ser_count,
                                  const SelectionVector &sel, idx_t offset) {
	auto list_data = ListVector::GetData(v);
	auto &child_vector = ListVector::GetEntry(v);
	idx_t list_entry_sizes[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < ser_count; i++) {
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx + offset);
		if (vdata.validity.RowIsValid(source_idx)) {
			auto list_entry = list_data[source_idx];

			// make room for list length, list validitymask
			entry_sizes[i] += sizeof(list_entry.length);
			entry_sizes[i] += (list_entry.length + 7) / 8;

			// serialize size of each entry (if non-constant size)
			if (!TypeIsConstantSize(ListType::GetChildType(v.GetType()).InternalType())) {
				entry_sizes[i] += list_entry.length * sizeof(list_entry.length);
			}

			// compute size of each the elements in list_entry and sum them
			auto entry_remaining = list_entry.length;
			auto entry_offset = list_entry.offset;
			while (entry_remaining > 0) {
				// the list entry can span multiple vectors
				auto next = MinValue((idx_t)STANDARD_VECTOR_SIZE, entry_remaining);

				// compute and add to the total
				std::fill_n(list_entry_sizes, next, 0);
				RowOperations::ComputeEntrySizes(child_vector, list_entry_sizes, next, next,
				                                 *FlatVector::IncrementalSelectionVector(), entry_offset);
				for (idx_t list_idx = 0; list_idx < next; list_idx++) {
					entry_sizes[i] += list_entry_sizes[list_idx];
				}

				// update for next iteration
				entry_remaining -= next;
				entry_offset += next;
			}
		}
	}
}

static void ComputeArrayEntrySizes(Vector &v, UnifiedVectorFormat &vdata, idx_t entry_sizes[], idx_t ser_count,
                                   const SelectionVector &sel, idx_t offset) {

	auto array_size = ArrayType::GetSize(v.GetType());
	auto child_vector = ArrayVector::GetEntry(v);

	idx_t array_entry_sizes[STANDARD_VECTOR_SIZE];
	const idx_t array_validitymask_size = (array_size + 7) / 8;

	for (idx_t i = 0; i < ser_count; i++) {

		// Validity for the array elements
		entry_sizes[i] += array_validitymask_size;

		// serialize size of each entry (if non-constant size)
		if (!TypeIsConstantSize(ArrayType::GetChildType(v.GetType()).InternalType())) {
			entry_sizes[i] += array_size * sizeof(idx_t);
		}

		auto elem_idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(elem_idx + offset);

		auto array_start = source_idx * array_size;
		auto elem_remaining = array_size;

		// the array could span multiple vectors, so we divide it into chunks
		while (elem_remaining > 0) {
			auto chunk_size = MinValue(static_cast<idx_t>(STANDARD_VECTOR_SIZE), elem_remaining);

			// compute and add to the total
			std::fill_n(array_entry_sizes, chunk_size, 0);
			RowOperations::ComputeEntrySizes(child_vector, array_entry_sizes, chunk_size, chunk_size,
			                                 *FlatVector::IncrementalSelectionVector(), array_start);
			for (idx_t arr_elem_idx = 0; arr_elem_idx < chunk_size; arr_elem_idx++) {
				entry_sizes[i] += array_entry_sizes[arr_elem_idx];
			}
			// update for next iteration
			elem_remaining -= chunk_size;
			array_start += chunk_size;
		}
	}
}

void RowOperations::ComputeEntrySizes(Vector &v, UnifiedVectorFormat &vdata, idx_t entry_sizes[], idx_t vcount,
                                      idx_t ser_count, const SelectionVector &sel, idx_t offset) {
	const auto physical_type = v.GetType().InternalType();
	if (TypeIsConstantSize(physical_type)) {
		const auto type_size = GetTypeIdSize(physical_type);
		for (idx_t i = 0; i < ser_count; i++) {
			entry_sizes[i] += type_size;
		}
	} else {
		switch (physical_type) {
		case PhysicalType::VARCHAR:
			ComputeStringEntrySizes(vdata, entry_sizes, ser_count, sel, offset);
			break;
		case PhysicalType::STRUCT:
			ComputeStructEntrySizes(v, entry_sizes, vcount, ser_count, sel, offset);
			break;
		case PhysicalType::LIST:
			ComputeListEntrySizes(v, vdata, entry_sizes, ser_count, sel, offset);
			break;
		case PhysicalType::ARRAY:
			ComputeArrayEntrySizes(v, vdata, entry_sizes, ser_count, sel, offset);
			break;
		default:
			// LCOV_EXCL_START
			throw NotImplementedException("Column with variable size type %s cannot be serialized to row-format",
			                              v.GetType().ToString());
			// LCOV_EXCL_STOP
		}
	}
}

void RowOperations::ComputeEntrySizes(Vector &v, idx_t entry_sizes[], idx_t vcount, idx_t ser_count,
                                      const SelectionVector &sel, idx_t offset) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);
	ComputeEntrySizes(v, vdata, entry_sizes, vcount, ser_count, sel, offset);
}

template <class T>
static void TemplatedHeapScatter(UnifiedVectorFormat &vdata, const SelectionVector &sel, idx_t count,
                                 data_ptr_t *key_locations, optional_ptr<NestedValidity> parent_validity,
                                 idx_t offset) {
	auto source = UnifiedVectorFormat::GetData<T>(vdata);
	if (!parent_validity) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx + offset);

			auto target = (T *)key_locations[i];
			Store<T>(source[source_idx], data_ptr_cast(target));
			key_locations[i] += sizeof(T);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx + offset);

			auto target = (T *)key_locations[i];
			Store<T>(source[source_idx], data_ptr_cast(target));
			key_locations[i] += sizeof(T);

			// set the validitymask
			if (!vdata.validity.RowIsValid(source_idx)) {
				parent_validity->SetInvalid(i);
			}
		}
	}
}

static void HeapScatterStringVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
                                    data_ptr_t *key_locations, optional_ptr<NestedValidity> parent_validity,
                                    idx_t offset) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);

	auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);
	if (!parent_validity) {
		for (idx_t i = 0; i < ser_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx + offset);
			if (vdata.validity.RowIsValid(source_idx)) {
				auto &string_entry = strings[source_idx];
				// store string size
				Store<uint32_t>(NumericCast<uint32_t>(string_entry.GetSize()), key_locations[i]);
				key_locations[i] += sizeof(uint32_t);
				// store the string
				memcpy(key_locations[i], string_entry.GetData(), string_entry.GetSize());
				key_locations[i] += string_entry.GetSize();
			}
		}
	} else {
		for (idx_t i = 0; i < ser_count; i++) {
			auto idx = sel.get_index(i);
			auto source_idx = vdata.sel->get_index(idx + offset);
			if (vdata.validity.RowIsValid(source_idx)) {
				auto &string_entry = strings[source_idx];
				// store string size
				Store<uint32_t>(NumericCast<uint32_t>(string_entry.GetSize()), key_locations[i]);
				key_locations[i] += sizeof(uint32_t);
				// store the string
				memcpy(key_locations[i], string_entry.GetData(), string_entry.GetSize());
				key_locations[i] += string_entry.GetSize();
			} else {
				// set the validitymask
				parent_validity->SetInvalid(i);
			}
		}
	}
}

static void HeapScatterStructVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
                                    data_ptr_t *key_locations, optional_ptr<NestedValidity> parent_validity,
                                    idx_t offset) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);

	auto &children = StructVector::GetEntries(v);
	idx_t num_children = children.size();

	// struct must have a validitymask for its fields
	const idx_t struct_validitymask_size = (num_children + 7) / 8;
	data_ptr_t struct_validitymask_locations[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < ser_count; i++) {
		// initialize the struct validity mask
		struct_validitymask_locations[i] = key_locations[i];
		memset(struct_validitymask_locations[i], -1, struct_validitymask_size);
		key_locations[i] += struct_validitymask_size;

		// set whether the whole struct is null
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx) + offset;
		if (parent_validity && !vdata.validity.RowIsValid(source_idx)) {
			parent_validity->SetInvalid(i);
		}
	}

	// now serialize the struct vectors
	for (idx_t i = 0; i < children.size(); i++) {
		auto &struct_vector = *children[i];
		NestedValidity struct_validity(struct_validitymask_locations, i);
		RowOperations::HeapScatter(struct_vector, vcount, sel, ser_count, key_locations, &struct_validity, offset);
	}
}

static void HeapScatterListVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
                                  data_ptr_t *key_locations, optional_ptr<NestedValidity> parent_validity,
                                  idx_t offset) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);

	auto list_data = ListVector::GetData(v);
	auto &child_vector = ListVector::GetEntry(v);

	UnifiedVectorFormat list_vdata;
	child_vector.ToUnifiedFormat(ListVector::GetListSize(v), list_vdata);
	auto child_type = ListType::GetChildType(v.GetType()).InternalType();

	idx_t list_entry_sizes[STANDARD_VECTOR_SIZE];
	data_ptr_t list_entry_locations[STANDARD_VECTOR_SIZE];

	for (idx_t i = 0; i < ser_count; i++) {
		auto idx = sel.get_index(i);
		auto source_idx = vdata.sel->get_index(idx + offset);
		if (!vdata.validity.RowIsValid(source_idx)) {
			if (parent_validity) {
				// set the row validitymask for this column to invalid
				parent_validity->SetInvalid(i);
			}
			continue;
		}
		auto list_entry = list_data[source_idx];

		// store list length
		Store<uint64_t>(list_entry.length, key_locations[i]);
		key_locations[i] += sizeof(list_entry.length);

		// make room for the validitymask
		data_ptr_t list_validitymask_location = key_locations[i];
		idx_t entry_offset_in_byte = 0;
		idx_t validitymask_size = (list_entry.length + 7) / 8;
		memset(list_validitymask_location, -1, validitymask_size);
		key_locations[i] += validitymask_size;

		// serialize size of each entry (if non-constant size)
		data_ptr_t var_entry_size_ptr = nullptr;
		if (!TypeIsConstantSize(child_type)) {
			var_entry_size_ptr = key_locations[i];
			key_locations[i] += list_entry.length * sizeof(idx_t);
		}

		auto entry_remaining = list_entry.length;
		auto entry_offset = list_entry.offset;
		while (entry_remaining > 0) {
			// the list entry can span multiple vectors
			auto next = MinValue((idx_t)STANDARD_VECTOR_SIZE, entry_remaining);

			// serialize list validity
			for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
				auto list_idx = list_vdata.sel->get_index(entry_idx + entry_offset);
				if (!list_vdata.validity.RowIsValid(list_idx)) {
					*(list_validitymask_location) &= ~(1UL << entry_offset_in_byte);
				}
				if (++entry_offset_in_byte == 8) {
					list_validitymask_location++;
					entry_offset_in_byte = 0;
				}
			}

			if (TypeIsConstantSize(child_type)) {
				// constant size list entries: set list entry locations
				const idx_t type_size = GetTypeIdSize(child_type);
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += type_size;
				}
			} else {
				// variable size list entries: compute entry sizes and set list entry locations
				std::fill_n(list_entry_sizes, next, 0);
				RowOperations::ComputeEntrySizes(child_vector, list_entry_sizes, next, next,
				                                 *FlatVector::IncrementalSelectionVector(), entry_offset);
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += list_entry_sizes[entry_idx];
					Store<idx_t>(list_entry_sizes[entry_idx], var_entry_size_ptr);
					var_entry_size_ptr += sizeof(idx_t);
				}
			}

			// now serialize to the locations
			RowOperations::HeapScatter(child_vector, ListVector::GetListSize(v),
			                           *FlatVector::IncrementalSelectionVector(), next, list_entry_locations, nullptr,
			                           entry_offset);

			// update for next iteration
			entry_remaining -= next;
			entry_offset += next;
		}
	}
}

static void HeapScatterArrayVector(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
                                   data_ptr_t *key_locations, optional_ptr<NestedValidity> parent_validity,
                                   idx_t offset) {

	auto &child_vector = ArrayVector::GetEntry(v);
	auto array_size = ArrayType::GetSize(v.GetType());
	auto child_type = ArrayType::GetChildType(v.GetType());
	auto child_type_size = GetTypeIdSize(child_type.InternalType());
	auto child_type_is_var_size = !TypeIsConstantSize(child_type.InternalType());

	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(vcount, vdata);

	UnifiedVectorFormat child_vdata;
	child_vector.ToUnifiedFormat(ArrayVector::GetTotalSize(v), child_vdata);

	data_ptr_t array_entry_locations[STANDARD_VECTOR_SIZE];
	idx_t array_entry_sizes[STANDARD_VECTOR_SIZE];

	// array must have a validitymask for its elements
	auto array_validitymask_size = (array_size + 7) / 8;

	for (idx_t i = 0; i < ser_count; i++) {
		// Set if the whole array itself is null in the parent entry
		auto source_idx = vdata.sel->get_index(sel.get_index(i) + offset);
		if (parent_validity && !vdata.validity.RowIsValid(source_idx)) {
			parent_validity->SetInvalid(i);
		}

		// Now we can serialize the array itself
		// Every array starts with a validity mask for the children
		data_ptr_t array_validitymask_location = key_locations[i];
		memset(array_validitymask_location, -1, array_validitymask_size);
		key_locations[i] += array_validitymask_size;

		NestedValidity array_parent_validity(array_validitymask_location);

		// If the array contains variable size entries, we reserve spaces for them here
		data_ptr_t var_entry_size_ptr = nullptr;
		if (child_type_is_var_size) {
			var_entry_size_ptr = key_locations[i];
			key_locations[i] += array_size * sizeof(idx_t);
		}

		// Then comes the elements
		auto array_start = source_idx * array_size;
		auto elem_remaining = array_size;

		while (elem_remaining > 0) {
			// the array elements can span multiple vectors, so we divide it into chunks
			auto chunk_size = MinValue(static_cast<idx_t>(STANDARD_VECTOR_SIZE), elem_remaining);

			// Setup the locations for the elements
			if (child_type_is_var_size) {
				// The elements are variable sized
				std::fill_n(array_entry_sizes, chunk_size, 0);
				RowOperations::ComputeEntrySizes(child_vector, array_entry_sizes, chunk_size, chunk_size,
				                                 *FlatVector::IncrementalSelectionVector(), array_start);
				for (idx_t elem_idx = 0; elem_idx < chunk_size; elem_idx++) {
					array_entry_locations[elem_idx] = key_locations[i];
					key_locations[i] += array_entry_sizes[elem_idx];

					// Now store the size of the entry
					Store<idx_t>(array_entry_sizes[elem_idx], var_entry_size_ptr);
					var_entry_size_ptr += sizeof(idx_t);
				}
			} else {
				// The elements are constant sized
				for (idx_t elem_idx = 0; elem_idx < chunk_size; elem_idx++) {
					array_entry_locations[elem_idx] = key_locations[i];
					key_locations[i] += child_type_size;
				}
			}

			RowOperations::HeapScatter(child_vector, ArrayVector::GetTotalSize(v),
			                           *FlatVector::IncrementalSelectionVector(), chunk_size, array_entry_locations,
			                           &array_parent_validity, array_start);

			// update for next iteration
			elem_remaining -= chunk_size;
			array_start += chunk_size;
			array_parent_validity.OffsetListBy(chunk_size);
		}
	}
}

void RowOperations::HeapScatter(Vector &v, idx_t vcount, const SelectionVector &sel, idx_t ser_count,
                                data_ptr_t *key_locations, optional_ptr<NestedValidity> parent_validity, idx_t offset) {
	if (TypeIsConstantSize(v.GetType().InternalType())) {
		UnifiedVectorFormat vdata;
		v.ToUnifiedFormat(vcount, vdata);
		RowOperations::HeapScatterVData(vdata, v.GetType().InternalType(), sel, ser_count, key_locations,
		                                parent_validity, offset);
	} else {
		switch (v.GetType().InternalType()) {
		case PhysicalType::VARCHAR:
			HeapScatterStringVector(v, vcount, sel, ser_count, key_locations, parent_validity, offset);
			break;
		case PhysicalType::STRUCT:
			HeapScatterStructVector(v, vcount, sel, ser_count, key_locations, parent_validity, offset);
			break;
		case PhysicalType::LIST:
			HeapScatterListVector(v, vcount, sel, ser_count, key_locations, parent_validity, offset);
			break;
		case PhysicalType::ARRAY:
			HeapScatterArrayVector(v, vcount, sel, ser_count, key_locations, parent_validity, offset);
			break;
		default:
			// LCOV_EXCL_START
			throw NotImplementedException("Serialization of variable length vector with type %s",
			                              v.GetType().ToString());
			// LCOV_EXCL_STOP
		}
	}
}

void RowOperations::HeapScatterVData(UnifiedVectorFormat &vdata, PhysicalType type, const SelectionVector &sel,
                                     idx_t ser_count, data_ptr_t *key_locations,
                                     optional_ptr<NestedValidity> parent_validity, idx_t offset) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedHeapScatter<int8_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::INT16:
		TemplatedHeapScatter<int16_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::INT32:
		TemplatedHeapScatter<int32_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::INT64:
		TemplatedHeapScatter<int64_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::UINT8:
		TemplatedHeapScatter<uint8_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::UINT16:
		TemplatedHeapScatter<uint16_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::UINT32:
		TemplatedHeapScatter<uint32_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::UINT64:
		TemplatedHeapScatter<uint64_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::INT128:
		TemplatedHeapScatter<hugeint_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::UINT128:
		TemplatedHeapScatter<uhugeint_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::FLOAT:
		TemplatedHeapScatter<float>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::DOUBLE:
		TemplatedHeapScatter<double>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	case PhysicalType::INTERVAL:
		TemplatedHeapScatter<interval_t>(vdata, sel, ser_count, key_locations, parent_validity, offset);
		break;
	default:
		throw NotImplementedException("FIXME: Serialize to of constant type column to row-format");
	}
}

} // namespace duckdb
