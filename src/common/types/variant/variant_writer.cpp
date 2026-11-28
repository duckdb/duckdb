#include "duckdb/common/types/variant_writer.hpp"

namespace duckdb {

VariantWriterState::VariantWriterState(ArenaAllocator &keys_arena)
    : AuxiliaryDataHolder(AuxiliaryDataType::VARIANT_WRITER_STATE), builder(dictionary, keys_arena) {
	builder.dictionary_keys = &dictionary_keys;
}

idx_t VariantWriterState::GetAllocationSize() const {
	const auto ids =
	    builder.child_key_ids.capacity() + builder.child_value_ids.capacity() + builder.key_slots.capacity();
	return builder.type_ids.capacity() * sizeof(uint8_t) + (builder.byte_offsets.capacity() + ids) * sizeof(uint32_t) +
	       builder.blob.capacity() + dictionary_keys.capacity() * sizeof(string_t);
}

VectorWriter<VectorVariantType>::VectorWriter(Vector &vector, idx_t count, idx_t offset)
    : variant(vector), count(offset + count), current_idx(offset) {
}

VectorWriter<VectorVariantType>::VectorWriter(VectorWriter &&other) noexcept
    : variant(other.variant), state(other.state), count(other.count), current_idx(other.current_idx),
      keys_data(other.keys_data), children_data(other.children_data), values_data(other.values_data),
      blob_data(other.blob_data), blob_heap(other.blob_heap), type_ids_data(other.type_ids_data),
      byte_offset_data(other.byte_offset_data), values_size(other.values_size), values_capacity(other.values_capacity),
      keys_index_data(other.keys_index_data), values_index_data(other.values_index_data),
      keys_index_validity(other.keys_index_validity), children_size(other.children_size),
      children_capacity(other.children_capacity), keys_entry_data(other.keys_entry_data), keys_size(other.keys_size),
      keys_capacity(other.keys_capacity) {
	other.state = nullptr;
	other.count = other.current_idx;
}

VectorWriter<VectorVariantType>::~VectorWriter() { // NOLINT(bugprone-exception-escape): the lists were reserved already
	D_ASSERT(Exception::UncaughtException() || current_idx == count);
	if (!state) {
		return;
	}
	ListVector::SetListSize(VariantVector::GetKeys(variant), keys_size);
	ListVector::SetListSize(VariantVector::GetChildren(variant), children_size);
	ListVector::SetListSize(VariantVector::GetValues(variant), values_size);
}

void VectorWriter<VectorVariantType>::Initialize() {
	auto &keys = VariantVector::GetKeys(variant);
	auto &keys_entry = ListVector::GetChildMutable(keys);
	//! BuildVariant leaves the keys as a dictionary, the new keys are written flat after the existing ones
	if (keys_entry.GetVectorType() != VectorType::FLAT_VECTOR) {
		keys_entry.Flatten();
	}
	//! the state lives as long as the heap its dictionary keys are allocated in
	auto &keys_buffer = StringVector::GetStringBuffer(keys_entry);
	auto &keys_arena = keys_buffer.GetStringAllocator();
	auto &auxiliary_data = keys_buffer.GetAuxiliaryData();
	if (auxiliary_data) {
		for (auto &entry : auxiliary_data->data) {
			if (entry->type == AuxiliaryDataType::VARIANT_WRITER_STATE) {
				state = &static_cast<VariantWriterState &>(*entry);
				break;
			}
		}
	}
	if (!state) {
		auto new_state = make_uniq<VariantWriterState>(keys_arena);
		state = new_state.get();
		keys_buffer.AddAuxiliaryData(std::move(new_state));
	}

	keys_size = ListVector::GetListSize(keys);
	children_size = ListVector::GetListSize(VariantVector::GetChildren(variant));
	values_size = ListVector::GetListSize(VariantVector::GetValues(variant));
	RefreshRows();
	RefreshChildren();
	//! every row has at least one value
	ListVector::Reserve(VariantVector::GetValues(variant), values_size + count - current_idx);
	RefreshValues();
}

void VectorWriter<VectorVariantType>::RefreshRows() {
	if (!state) {
		return;
	}
	keys_data = FlatVector::GetDataMutable<list_entry_t>(VariantVector::GetKeys(variant));
	children_data = FlatVector::GetDataMutable<list_entry_t>(VariantVector::GetChildren(variant));
	values_data = FlatVector::GetDataMutable<list_entry_t>(VariantVector::GetValues(variant));
	blob_data = FlatVector::GetDataMutable<string_t>(VariantVector::GetData(variant));
}

void VectorWriter<VectorVariantType>::RefreshValues() {
	values_capacity = ListVector::GetListCapacity(VariantVector::GetValues(variant));
	type_ids_data = FlatVector::GetDataMutable<uint8_t>(VariantVector::GetValuesTypeId(variant));
	byte_offset_data = FlatVector::GetDataMutable<uint32_t>(VariantVector::GetValuesByteOffset(variant));
}

void VectorWriter<VectorVariantType>::RefreshChildren() {
	children_capacity = ListVector::GetListCapacity(VariantVector::GetChildren(variant));
	auto &keys_index = VariantVector::GetChildrenKeysIndex(variant);
	keys_index_data = FlatVector::GetDataMutable<uint32_t>(keys_index);
	keys_index_validity = &FlatVector::ValidityMutable(keys_index);
	values_index_data = FlatVector::GetDataMutable<uint32_t>(VariantVector::GetChildrenValuesIndex(variant));

	auto &keys = VariantVector::GetKeys(variant);
	keys_capacity = ListVector::GetListCapacity(keys);
	keys_entry_data = FlatVector::GetDataMutable<string_t>(ListVector::GetChildMutable(keys));
}

void VectorWriter<VectorVariantType>::ReserveValues(idx_t required) {
	ListVector::Reserve(VariantVector::GetValues(variant), required);
	RefreshValues();
}

//! Grows the keys first, the lists with the largest entries are copied while the other lists are still small
void VectorWriter<VectorVariantType>::ReserveChildren(idx_t child_count, idx_t key_count) {
	if (keys_size + key_count > keys_capacity) {
		ListVector::Reserve(VariantVector::GetKeys(variant), keys_size + key_count);
	}
	if (children_size + child_count > children_capacity) {
		ListVector::Reserve(VariantVector::GetChildren(variant), children_size + child_count);
	}
	RefreshChildren();
}

void VectorWriter<VectorVariantType>::WriteChildren() {
	auto &builder = state->builder;
	const auto child_count = builder.child_value_ids.size();
	const auto key_count = builder.key_slots.size();
	children_data[current_idx] = list_entry_t(children_size, child_count);
	keys_data[current_idx] = list_entry_t(keys_size, key_count);

	const auto child_value_ids = builder.child_value_ids.data();
	const auto child_key_ids = builder.child_key_ids.data();
	for (idx_t i = 0; i < child_count; i++) {
		const auto child_idx = children_size + i;
		values_index_data[child_idx] = child_value_ids[i];
		if (child_key_ids[i] == VARIANT_INVALID_KEY) {
			keys_index_validity->SetInvalid(child_idx);
		} else {
			keys_index_data[child_idx] = child_key_ids[i];
		}
	}
	children_size += child_count;

	//! every distinct key is copied into the heap of the keys vector once, when it is added to the dictionary
	const auto key_slots = builder.key_slots.data();
	const auto dictionary_keys = state->dictionary_keys.data();
	for (idx_t i = 0; i < key_count; i++) {
		keys_entry_data[keys_size + i] = dictionary_keys[key_slots[i]];
	}
	keys_size += key_count;
}

void VectorWriter<VectorVariantType>::WriteNullRow() {
	keys_data[current_idx] = list_entry_t(keys_size, 0);
	children_data[current_idx] = list_entry_t(children_size, 0);
	values_data[current_idx] = list_entry_t(values_size, 0);
	blob_data[current_idx] = string_t(0U);
	FlatVector::SetNull(variant, current_idx, true);
	current_idx++;
}

StringHeap &VectorWriter<VectorVariantType>::InitializeBlobHeap() {
	blob_heap = StringVector::GetStringHeap(VariantVector::GetData(variant));
	return *blob_heap;
}

void VectorWriter<VectorVariantType>::ThrowRowTooLarge() {
	throw InvalidInputException(
	    "Cannot convert value to VARIANT: encoded row size exceeds the maximum supported %u bytes",
	    NumericLimits<uint32_t>::Maximum());
}

void VectorWriter<VectorVariantType>::EmitContainer(const VariantNode &node, VariantBuilder &builder) {
	EmitIterator(node, builder);
}

} // namespace duckdb
