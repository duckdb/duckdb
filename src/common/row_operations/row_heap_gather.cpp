#include "duckdb/common/helper.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

using ValidityBytes = TemplatedValidityMask<uint8_t>;

template <class T>
static void TemplatedHeapGather(Vector &v, const idx_t count, const SelectionVector &sel, data_ptr_t *key_locations) {
	auto target = FlatVector::GetData<T>(v);

	for (idx_t i = 0; i < count; ++i) {
		const auto col_idx = sel.get_index(i);
		target[col_idx] = Load<T>(key_locations[i]);
		key_locations[i] += sizeof(T);
	}
}

static void HeapGatherStringVector(Vector &v, const idx_t vcount, const SelectionVector &sel,
                                   data_ptr_t *key_locations) {
	const auto &validity = FlatVector::Validity(v);
	auto target = FlatVector::GetData<string_t>(v);

	for (idx_t i = 0; i < vcount; i++) {
		const auto col_idx = sel.get_index(i);
		if (!validity.RowIsValid(col_idx)) {
			continue;
		}
		auto len = Load<uint32_t>(key_locations[i]);
		key_locations[i] += sizeof(uint32_t);
		target[col_idx] = StringVector::AddStringOrBlob(v, string_t(const_char_ptr_cast(key_locations[i]), len));
		key_locations[i] += len;
	}
}

static void HeapGatherStructVector(Vector &v, const idx_t vcount, const SelectionVector &sel,
                                   data_ptr_t *key_locations) {
	// struct must have a validitymask for its fields
	auto &child_types = StructType::GetChildTypes(v.GetType());
	const idx_t struct_validitymask_size = (child_types.size() + 7) / 8;
	data_ptr_t struct_validitymask_locations[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < vcount; i++) {
		// use key_locations as the validitymask, and create struct_key_locations
		struct_validitymask_locations[i] = key_locations[i];
		key_locations[i] += struct_validitymask_size;
	}

	// now deserialize into the struct vectors
	auto &children = StructVector::GetEntries(v);
	for (idx_t i = 0; i < child_types.size(); i++) {
		NestedValidity parent_validity(struct_validitymask_locations, i);
		RowOperations::HeapGather(*children[i], vcount, sel, key_locations, &parent_validity);
	}
}

static void HeapGatherListVector(Vector &v, const idx_t vcount, const SelectionVector &sel, data_ptr_t *key_locations) {
	const auto &validity = FlatVector::Validity(v);

	auto child_type = ListType::GetChildType(v.GetType());
	auto list_data = ListVector::GetData(v);
	data_ptr_t list_entry_locations[STANDARD_VECTOR_SIZE];

	uint64_t entry_offset = ListVector::GetListSize(v);
	for (idx_t i = 0; i < vcount; i++) {
		const auto col_idx = sel.get_index(i);
		if (!validity.RowIsValid(col_idx)) {
			continue;
		}
		// read list length
		auto entry_remaining = Load<uint64_t>(key_locations[i]);
		key_locations[i] += sizeof(uint64_t);
		// set list entry attributes
		list_data[col_idx].length = entry_remaining;
		list_data[col_idx].offset = entry_offset;
		// skip over the validity mask
		data_ptr_t validitymask_location = key_locations[i];
		idx_t offset_in_byte = 0;
		key_locations[i] += (entry_remaining + 7) / 8;
		// entry sizes
		data_ptr_t var_entry_size_ptr = nullptr;
		if (!TypeIsConstantSize(child_type.InternalType())) {
			var_entry_size_ptr = key_locations[i];
			key_locations[i] += entry_remaining * sizeof(idx_t);
		}

		// now read the list data
		while (entry_remaining > 0) {
			auto next = MinValue(entry_remaining, (idx_t)STANDARD_VECTOR_SIZE);

			// initialize a new vector to append
			Vector append_vector(v.GetType());
			append_vector.SetVectorType(v.GetVectorType());

			auto &list_vec_to_append = ListVector::GetEntry(append_vector);

			// set validity
			//! Since we are constructing the vector, this will always be a flat vector.
			auto &append_validity = FlatVector::Validity(list_vec_to_append);
			for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
				append_validity.Set(entry_idx, *(validitymask_location) & (1 << offset_in_byte));
				if (++offset_in_byte == 8) {
					validitymask_location++;
					offset_in_byte = 0;
				}
			}

			// compute entry sizes and set locations where the list entries are
			if (TypeIsConstantSize(child_type.InternalType())) {
				// constant size list entries
				const idx_t type_size = GetTypeIdSize(child_type.InternalType());
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += type_size;
				}
			} else {
				// variable size list entries
				for (idx_t entry_idx = 0; entry_idx < next; entry_idx++) {
					list_entry_locations[entry_idx] = key_locations[i];
					key_locations[i] += Load<idx_t>(var_entry_size_ptr);
					var_entry_size_ptr += sizeof(idx_t);
				}
			}

			// now deserialize and add to listvector
			RowOperations::HeapGather(list_vec_to_append, next, *FlatVector::IncrementalSelectionVector(),
			                          list_entry_locations, nullptr);
			ListVector::Append(v, list_vec_to_append, next);

			// update for next iteration
			entry_remaining -= next;
			entry_offset += next;
		}
	}
}

static void HeapGatherArrayVector(Vector &v, const idx_t vcount, const SelectionVector &sel,
                                  data_ptr_t *key_locations) {
	// Setup
	auto &child_type = ArrayType::GetChildType(v.GetType());
	auto array_size = ArrayType::GetSize(v.GetType());
	auto &child_vector = ArrayVector::GetEntry(v);
	auto child_type_size = GetTypeIdSize(child_type.InternalType());
	auto child_type_is_var_size = !TypeIsConstantSize(child_type.InternalType());

	data_ptr_t array_entry_locations[STANDARD_VECTOR_SIZE];

	// array must have a validitymask for its elements
	auto array_validitymask_size = (array_size + 7) / 8;

	for (idx_t i = 0; i < vcount; i++) {
		// Setup validity mask
		data_ptr_t array_validitymask_location = key_locations[i];
		key_locations[i] += array_validitymask_size;

		NestedValidity parent_validity(array_validitymask_location);

		// The size of each variable size entry is stored after the validity mask
		// (if the child type is variable size)
		data_ptr_t var_entry_size_ptr = nullptr;
		if (child_type_is_var_size) {
			var_entry_size_ptr = key_locations[i];
			key_locations[i] += array_size * sizeof(idx_t);
		}

		// row idx
		const auto row_idx = sel.get_index(i);

		idx_t array_start = row_idx * array_size;
		idx_t elem_remaining = array_size;

		while (elem_remaining > 0) {
			auto chunk_size = MinValue(static_cast<idx_t>(STANDARD_VECTOR_SIZE), elem_remaining);

			SelectionVector array_sel(STANDARD_VECTOR_SIZE);

			if (child_type_is_var_size) {
				// variable size list entries
				for (idx_t elem_idx = 0; elem_idx < chunk_size; elem_idx++) {
					array_entry_locations[elem_idx] = key_locations[i];
					key_locations[i] += Load<idx_t>(var_entry_size_ptr);
					var_entry_size_ptr += sizeof(idx_t);
					array_sel.set_index(elem_idx, array_start + elem_idx);
				}
			} else {
				// constant size list entries
				for (idx_t elem_idx = 0; elem_idx < chunk_size; elem_idx++) {
					array_entry_locations[elem_idx] = key_locations[i];
					key_locations[i] += child_type_size;
					array_sel.set_index(elem_idx, array_start + elem_idx);
				}
			}

			// Pass on this array's validity mask to the child vector
			RowOperations::HeapGather(child_vector, chunk_size, array_sel, array_entry_locations, &parent_validity);

			elem_remaining -= chunk_size;
			array_start += chunk_size;
			parent_validity.OffsetListBy(chunk_size);
		}
	}
}

void RowOperations::HeapGather(Vector &v, const idx_t &vcount, const SelectionVector &sel, data_ptr_t *key_locations,
                               optional_ptr<NestedValidity> parent_validity) {
	v.SetVectorType(VectorType::FLAT_VECTOR);

	auto &validity = FlatVector::Validity(v);
	if (parent_validity) {
		for (idx_t i = 0; i < vcount; i++) {
			const auto valid = parent_validity->IsValid(i);
			const auto col_idx = sel.get_index(i);
			validity.Set(col_idx, valid);
		}
	}

	auto type = v.GetType().InternalType();
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedHeapGather<int8_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT16:
		TemplatedHeapGather<int16_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT32:
		TemplatedHeapGather<int32_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT64:
		TemplatedHeapGather<int64_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT8:
		TemplatedHeapGather<uint8_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT16:
		TemplatedHeapGather<uint16_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT32:
		TemplatedHeapGather<uint32_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT64:
		TemplatedHeapGather<uint64_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INT128:
		TemplatedHeapGather<hugeint_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::UINT128:
		TemplatedHeapGather<uhugeint_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::FLOAT:
		TemplatedHeapGather<float>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::DOUBLE:
		TemplatedHeapGather<double>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::INTERVAL:
		TemplatedHeapGather<interval_t>(v, vcount, sel, key_locations);
		break;
	case PhysicalType::VARCHAR:
		HeapGatherStringVector(v, vcount, sel, key_locations);
		break;
	case PhysicalType::STRUCT:
		HeapGatherStructVector(v, vcount, sel, key_locations);
		break;
	case PhysicalType::LIST:
		HeapGatherListVector(v, vcount, sel, key_locations);
		break;
	case PhysicalType::ARRAY:
		HeapGatherArrayVector(v, vcount, sel, key_locations);
		break;
	default:
		throw NotImplementedException("Unimplemented deserialize from row-format");
	}
}

} // namespace duckdb
