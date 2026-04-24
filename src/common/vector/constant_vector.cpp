#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

namespace duckdb {

buffer_ptr<VectorBuffer> CreateConstantBuffer(const Value &value, count_t count) {
	auto &type = value.type();
	auto internal_type = type.InternalType();
	buffer_ptr<VectorBuffer> result;
	capacity_t capacity(1ULL);
	if (internal_type == PhysicalType::STRUCT) {
		result = make_buffer<VectorStructBuffer>(value.type(), capacity);
	} else if (internal_type == PhysicalType::LIST) {
		result = make_buffer<VectorListBuffer>(capacity, value.type());
	} else if (internal_type == PhysicalType::ARRAY) {
		result = make_buffer<VectorArrayBuffer>(value.type(), capacity);
	} else if (internal_type == PhysicalType::VARCHAR) {
		result = make_buffer<VectorStringBuffer>(capacity);
	} else {
		result = make_buffer<StandardVectorBuffer>(capacity, GetTypeIdSize(internal_type));
	}
	result->SetValue(type, 0, value);
	result->SetVectorType(VectorType::CONSTANT_VECTOR);
	result->SetVectorSize(count);
	return result;
}

void ConstantVector::Reference(Vector &vector, const Value &value, count_t count) {
	D_ASSERT(vector.GetType() == value.type());
	vector.SetBuffer(CreateConstantBuffer(value, count));
}

void ConstantVector::SetNull(Vector &vector) {
	// try to re-use the buffer if possible
	auto &buffer_ref = vector.GetBufferRef();
	bool needs_new_buffer = !buffer_ref;
	if (!needs_new_buffer) {
		auto buffer_type = buffer_ref->GetBufferType();
		needs_new_buffer =
		    (buffer_type != VectorBufferType::STANDARD_BUFFER && buffer_type != VectorBufferType::STRUCT_BUFFER &&
		     buffer_type != VectorBufferType::ARRAY_BUFFER && buffer_type != VectorBufferType::LIST_BUFFER &&
		     buffer_type != VectorBufferType::STRING_BUFFER);
	}
	if (needs_new_buffer) {
		// we cannot re-use the buffer - refer a null-value which has code necessary to create a new buffer
		Reference(vector, Value(vector.GetType()), count_t(1ULL));
		return;
	}
	vector.SetVectorType(VectorType::CONSTANT_VECTOR);
	SetNull(vector, true);
}

void ConstantVector::SetNull(Vector &vector, bool is_null) {
	D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	auto &validity = vector.BufferMutable().GetValidityMask();
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
			auto &child = ArrayVector::GetChildMutable(vector);
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

void ConstantVector::Reference(Vector &vector, count_t count, const Vector &source, idx_t position,
                               idx_t source_count) {
	auto &source_type = source.GetType();
	switch (source_type.InternalType()) {
	case PhysicalType::LIST: {
		// retrieve the list entry from the source vector
		auto entries = source.Values<list_entry_t>(source_count);
		auto entry = entries[position];

		if (!entry.IsValid()) {
			// list is null: create null value
			SetNull(vector, count);
			break;
		}

		auto list_entry = entry.GetValue();

		// add the list entry as the first element of "vector"
		// FIXME: we only need to allocate space for 1 tuple here
		auto target_data = FlatVector::GetDataMutable<list_entry_t>(vector);
		target_data[0] = list_entry;

		// create a reference to the child list of the source vector
		auto &child = ListVector::GetChildMutable(vector);
		child.Reference(ListVector::GetChild(source));

		ListVector::SetListSize(vector, ListVector::GetListSize(source));
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		FlatVector::SetSize(vector, count);
		break;
	}
	case PhysicalType::ARRAY: {
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(source_count, vdata);
		auto source_idx = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(source_idx)) {
			// list is null: create null value
			SetNull(vector, count);
			break;
		}

		// Reference the child vector
		auto &target_child = ArrayVector::GetChildMutable(vector);
		auto &source_child = ArrayVector::GetChild(source);
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
		auto &validity = vector.BufferMutable().GetValidityMask();
		validity.Set(0, true);
		FlatVector::SetSize(vector, count);
		break;
	}
	case PhysicalType::STRUCT: {
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(source_count, vdata);

		auto struct_index = vdata.sel->get_index(position);
		if (!vdata.validity.RowIsValid(struct_index)) {
			// null struct: create null value
			SetNull(vector, count);
			break;
		}

		// struct: pass constant reference into child entries
		auto &source_entries = StructVector::GetEntries(source);
		auto &target_entries = StructVector::GetEntries(vector);
		for (idx_t i = 0; i < source_entries.size(); i++) {
			ConstantVector::Reference(target_entries[i], count, source_entries[i], position, source_count);
		}
		vector.SetVectorType(VectorType::CONSTANT_VECTOR);
		auto &validity = vector.BufferMutable().GetValidityMask();
		validity.Set(0, true);
		FlatVector::SetSize(vector, count);
		break;
	}
	default:
		// default behavior: get a value from the vector and reference it
		// this is not that expensive for scalar types
		auto value = source.GetValue(position);
		vector.Reference(value, count);
		D_ASSERT(vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
		break;
	}
}

} // namespace duckdb
