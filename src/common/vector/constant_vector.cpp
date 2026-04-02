#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector/string_vector.hpp"

namespace duckdb {

void ConstantVector::SetNull(Vector &vector) {
	vector.SetVectorType(VectorType::CONSTANT_VECTOR);
	SetNull(vector, true);
}

void ConstantVector::SetNull(Vector &vector, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	auto &validity = vector.buffer->GetValidityMask();
	validity.Set(0, !is_null);
	if (is_null) {
		auto &type = vector.GetType();
		auto internal_type = type.InternalType();
		if (internal_type == PhysicalType::STRUCT) {
			// set all child entries to null as well
			auto &entries = StructVector::GetEntries(vector);
			for (auto &entry : entries) {
				entry.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(entry, is_null);
			}
		} else if (internal_type == PhysicalType::ARRAY) {
			auto &child = ArrayVector::GetEntry(vector);
			D_ASSERT(child.GetVectorType() == VectorType::CONSTANT_VECTOR ||
			         child.GetVectorType() == VectorType::FLAT_VECTOR);
			auto array_size = ArrayType::GetSize(type);
			if (child.GetVectorType() == VectorType::CONSTANT_VECTOR) {
				D_ASSERT(array_size == 1);
				ConstantVector::SetNull(child, is_null);
			} else {
				for (idx_t i = 0; i < array_size; i++) {
					FlatVector::SetNull(child, i, is_null);
				}
			}
		}
	}
}

const SelectionVector *ConstantVector::ZeroSelectionVector(idx_t count, SelectionVector &owned_sel) {
	if (count <= STANDARD_VECTOR_SIZE) {
		return ConstantVector::ZeroSelectionVector();
	}
	owned_sel.Initialize(count);
	for (idx_t i = 0; i < count; i++) {
		owned_sel.set_index(i, 0);
	}
	return &owned_sel;
}

void ConstantVector::Reference(Vector &vector, Vector &source, idx_t position, idx_t count) {
	auto &source_type = source.GetType();
	switch (source_type.InternalType()) {
	case PhysicalType::LIST: {
		// retrieve the list entry from the source vector
		auto entries = source.Values<list_entry_t>(count);
		auto entry = entries[position];

		if (!entry.IsValid()) {
			// list is null: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		auto list_entry = entry.value;

		// add the list entry as the first element of "vector"
		// FIXME: we only need to allocate space for 1 tuple here
		auto target_data = FlatVector::GetData<list_entry_t>(vector);
		target_data[0] = list_entry;

		// create a reference to the child list of the source vector
		auto &child = ListVector::GetEntry(vector);
		child.Reference(ListVector::GetEntry(source));

		ListVector::SetListSize(vector, ListVector::GetListSize(source));
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		break;
	}
	case PhysicalType::ARRAY: {
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(count, vdata);
		auto source_idx = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(source_idx)) {
			// list is null: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		// Reference the child vector
		auto &target_child = ArrayVector::GetEntry(vector);
		auto &source_child = ArrayVector::GetEntry(source);
		target_child.Reference(source_child);

		// Only take the element at the given position
		auto array_size = ArrayType::GetSize(source_type);
		SelectionVector sel(array_size);
		for (idx_t i = 0; i < array_size; i++) {
			sel.set_index(i, array_size * source_idx + i);
		}
		target_child.Slice(sel, array_size);
		target_child.Flatten(array_size); // since its constant we only have to flatten this much

		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto &validity = vector.buffer->GetValidityMask();
		validity.Set(0, true);
		break;
	}
	case PhysicalType::STRUCT: {
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(count, vdata);

		auto struct_index = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(struct_index)) {
			// null struct: create null value
			Value null_value(source_type);
			vector.Reference(null_value);
			break;
		}

		// struct: pass constant reference into child entries
		auto &source_entries = StructVector::GetEntries(source);
		auto &target_entries = StructVector::GetEntries(vector);
		for (idx_t i = 0; i < source_entries.size(); i++) {
			ConstantVector::Reference(target_entries[i], source_entries[i], position, count);
		}
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto &validity = vector.buffer->GetValidityMask();
		validity.Set(0, true);
		break;
	}
	default:
		// default behavior: get a value from the vector and reference it
		// this is not that expensive for scalar types
		auto value = source.GetValue(position);
		vector.Reference(value);
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		break;
	}
}

template <class T>
static void TemplatedFlattenConstantVector(const Vector &const_vector, Vector &result, idx_t count) {
	auto constant = *ConstantVector::GetData<T>(const_vector);
	auto output = FlatVector::GetData<T>(result);
	for (idx_t i = 0; i < count; i++) {
		output[i] = constant;
	}
}

void ConstantVector::Flatten(const Vector &const_vector, Vector &result, idx_t count) {
	D_ASSERT(const_vector.GetType() == result.GetType());
	bool is_null = ConstantVector::IsNull(const_vector);
	auto &type = const_vector.GetType();
	if (is_null && type.InternalType() != PhysicalType::ARRAY) {
		// constant NULL, set nullmask
		auto &validity = FlatVector::Validity(result);
		validity.EnsureWritable();
		validity.SetAllInvalid(count);
		if (type.InternalType() != PhysicalType::STRUCT) {
			// for structs we still need to flatten the child vectors as well
			return;
		}
	}
	// non-null constant: have to repeat the constant
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		TemplatedFlattenConstantVector<bool>(const_vector, result, count);
		break;
	case PhysicalType::INT8:
		TemplatedFlattenConstantVector<int8_t>(const_vector, result, count);
		break;
	case PhysicalType::INT16:
		TemplatedFlattenConstantVector<int16_t>(const_vector, result, count);
		break;
	case PhysicalType::INT32:
		TemplatedFlattenConstantVector<int32_t>(const_vector, result, count);
		break;
	case PhysicalType::INT64:
		TemplatedFlattenConstantVector<int64_t>(const_vector, result, count);
		break;
	case PhysicalType::UINT8:
		TemplatedFlattenConstantVector<uint8_t>(const_vector, result, count);
		break;
	case PhysicalType::UINT16:
		TemplatedFlattenConstantVector<uint16_t>(const_vector, result, count);
		break;
	case PhysicalType::UINT32:
		TemplatedFlattenConstantVector<uint32_t>(const_vector, result, count);
		break;
	case PhysicalType::UINT64:
		TemplatedFlattenConstantVector<uint64_t>(const_vector, result, count);
		break;
	case PhysicalType::INT128:
		TemplatedFlattenConstantVector<hugeint_t>(const_vector, result, count);
		break;
	case PhysicalType::UINT128:
		TemplatedFlattenConstantVector<uhugeint_t>(const_vector, result, count);
		break;
	case PhysicalType::FLOAT:
		TemplatedFlattenConstantVector<float>(const_vector, result, count);
		break;
	case PhysicalType::DOUBLE:
		TemplatedFlattenConstantVector<double>(const_vector, result, count);
		break;
	case PhysicalType::INTERVAL:
		TemplatedFlattenConstantVector<interval_t>(const_vector, result, count);
		break;
	case PhysicalType::VARCHAR:
		TemplatedFlattenConstantVector<string_t>(const_vector, result, count);
		StringVector::AddHeapReference(result, const_vector);
		break;
	case PhysicalType::LIST: {
		TemplatedFlattenConstantVector<list_entry_t>(const_vector, result, count);

		// reference the child
		auto &original_child = ListVector::GetEntry(const_vector);
		auto &new_child = ListVector::GetEntry(result);
		new_child.Reference(original_child);
		ListVector::SetListSize(result, ListVector::GetListSize(const_vector));
		break;
	}
	case PhysicalType::ARRAY: {
		auto &original_child = ArrayVector::GetEntry(const_vector);
		auto array_size = ArrayType::GetSize(type);
		auto &new_child = ArrayVector::GetEntry(result);

		// Fast path: The array is a constant null
		if (is_null) {
			// Invalidate the parent array
			auto &validity = FlatVector::Validity(result);
			validity.SetAllInvalid(count);
			// Also invalidate the new child array
			FlatVector::Validity(new_child).SetAllInvalid(count * array_size);
			// Recurse
			new_child.Flatten(count * array_size);
			// TODO: the fast path should exit here, but the part below it is somehow required for correctness
			// Attach the flattened buffer and return
			// auxiliary = shared_ptr<VectorBuffer>(flattened_buffer.release());
			// return;
		}

		// Now we need to "unpack" the child vector.
		// Basically, do this:
		//
		// | a1 | | 1 |      | a1 | | 1 |
		//        | 2 |      | a2 | | 2 |
		//	             =>    ..   | 1 |
		//                          | 2 |
		// 							 ...

		// Create a selection vector
		SelectionVector sel(count * array_size);
		for (idx_t array_idx = 0; array_idx < count; array_idx++) {
			for (idx_t elem_idx = 0; elem_idx < array_size; elem_idx++) {
				auto position = array_idx * array_size + elem_idx;
				// Broadcast the validity
				if (FlatVector::IsNull(original_child, elem_idx)) {
					FlatVector::SetNull(new_child, position, true);
				}
				sel.set_index(position, elem_idx);
			}
		}

		// Copy over the data to the new buffer
		VectorOperations::Copy(original_child, new_child, sel, count * array_size, 0, 0);
		break;
	}
	case PhysicalType::STRUCT: {
		// struct: recursively flatten child entries
		auto &new_children = StructVector::GetEntries(result);
		auto &child_entries = StructVector::GetEntries(const_vector);
		for (idx_t child_idx = 0; child_idx < child_entries.size(); child_idx++) {
			ConstantVector::Flatten(child_entries[child_idx], new_children[child_idx], count);
		}
		break;
	}
	default:
		throw InternalException("Unimplemented type for VectorOperations::Flatten");
	}
}

} // namespace duckdb
