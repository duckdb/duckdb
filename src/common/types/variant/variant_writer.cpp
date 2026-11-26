#include "duckdb/common/types/variant_writer.hpp"

namespace duckdb {

namespace {

//! Copies the list entries of the rows to 'target', moved to start at 'offset'
void CopyListEntries(list_entry_t *target, const vector<list_entry_t> &entries, idx_t offset) {
	const auto count = entries.size();
	const auto source = entries.data();
	if (offset == 0) {
		memcpy(target, source, count * sizeof(list_entry_t));
		return;
	}
	for (idx_t i = 0; i < count; i++) {
		target[i] = list_entry_t(source[i].offset + offset, source[i].length);
	}
}

} // namespace

VectorWriter<VectorVariantType>::VectorWriter(Vector &vector, idx_t count, idx_t offset)
    : variant(vector), start_idx(offset), count(offset + count), current_idx(offset) {
}

VectorWriter<VectorVariantType>::VectorWriter(VectorWriter &&other) noexcept
    : variant(other.variant), state(std::move(other.state)), start_idx(other.start_idx), count(other.count),
      current_idx(other.current_idx) {
	other.count = other.current_idx;
}

VectorWriter<VectorVariantType>::~VectorWriter() { // NOLINT(bugprone-exception-escape): only the allocations can throw
	D_ASSERT(Exception::UncaughtException() || current_idx == count);
	if (state && !Exception::UncaughtException()) {
		Finalize();
	}
}

void VectorWriter<VectorVariantType>::InitializeState() {
	state = make_uniq<VariantWriterState>();
	const auto row_count = count - current_idx;
	state->keys.reserve(row_count);
	state->children.reserve(row_count);
	state->values.reserve(row_count);
	state->blobs.reserve(row_count);
}

StringHeap &VectorWriter<VectorVariantType>::InitializeBlobHeap() {
	state->blob_heap = StringVector::GetStringHeap(VariantVector::GetData(variant));
	return *state->blob_heap;
}

void VectorWriter<VectorVariantType>::ThrowRowTooLarge() {
	throw InvalidInputException(
	    "Cannot convert value to VARIANT: encoded row size exceeds the maximum supported %u bytes",
	    NumericLimits<uint32_t>::Maximum());
}

void VectorWriter<VectorVariantType>::EmitContainer(const VariantNode &node, VariantBuilder &builder) {
	EmitIterator(node, builder);
}

void VectorWriter<VectorVariantType>::Finalize() {
	auto &writer_state = *state;
	auto &builder = writer_state.builder;
	const auto row_count = writer_state.blobs.size();
	D_ASSERT(row_count == current_idx - start_idx);
	if (row_count == 0) {
		return;
	}

	auto &keys = VariantVector::GetKeys(variant);
	auto &keys_entry = ListVector::GetChildMutable(keys);
	auto &children = VariantVector::GetChildren(variant);
	auto &values = VariantVector::GetValues(variant);

	const auto keys_offset = ListVector::GetListSize(keys);
	const auto children_offset = ListVector::GetListSize(children);
	const auto values_offset = ListVector::GetListSize(values);
	const auto total_keys = builder.key_slots.size();
	const auto total_children = builder.child_value_ids.size();
	const auto total_values = builder.type_ids.size();

	//! BuildVariant leaves the keys as a dictionary, the new keys are written flat after the existing ones
	if (keys_entry.GetVectorType() != VectorType::FLAT_VECTOR) {
		keys_entry.Flatten();
	}
	ListVector::Reserve(keys, keys_offset + total_keys);
	ListVector::SetListSize(keys, keys_offset + total_keys);
	ListVector::Reserve(children, children_offset + total_children);
	ListVector::SetListSize(children, children_offset + total_children);
	ListVector::Reserve(values, values_offset + total_values);
	ListVector::SetListSize(values, values_offset + total_values);

	//! rows
	VariantVectorData variant_data(variant);
	CopyListEntries(variant_data.keys_data + start_idx, writer_state.keys, keys_offset);
	CopyListEntries(variant_data.children_data + start_idx, writer_state.children, children_offset);
	CopyListEntries(variant_data.values_data + start_idx, writer_state.values, values_offset);
	memcpy(variant_data.blob_data + start_idx, writer_state.blobs.data(), row_count * sizeof(string_t));
	for (auto row : writer_state.null_rows) {
		FlatVector::SetNull(variant, start_idx + row, true);
	}

	//! values
	if (total_values) {
		memcpy(variant_data.type_ids_data + values_offset, builder.type_ids.data(), total_values * sizeof(uint8_t));
		memcpy(variant_data.byte_offset_data + values_offset, builder.byte_offsets.data(),
		       total_values * sizeof(uint32_t));
	}

	//! children
	const auto child_value_ids = builder.child_value_ids.data();
	const auto child_key_ids = builder.child_key_ids.data();
	for (idx_t i = 0; i < total_children; i++) {
		const auto child_idx = children_offset + i;
		variant_data.values_index_data[child_idx] = child_value_ids[i];
		if (child_key_ids[i] == VARIANT_INVALID_KEY) {
			variant_data.keys_index_validity.SetInvalid(child_idx);
		} else {
			variant_data.keys_index_data[child_idx] = child_key_ids[i];
		}
	}

	//! keys: copy each distinct key into the keys vector once, then point every key slot at its copy
	vector<string_t> dictionary_keys(writer_state.dictionary.size());
	const auto dictionary_data = dictionary_keys.data();
	for (auto &entry : writer_state.dictionary) {
		dictionary_data[entry.second] = StringVector::AddStringOrBlob(keys_entry, entry.first);
	}
	const auto key_slots = builder.key_slots.data();
	auto keys_entry_data = FlatVector::GetDataMutable<string_t>(keys_entry);
	for (idx_t i = 0; i < total_keys; i++) {
		keys_entry_data[keys_offset + i] = dictionary_data[key_slots[i]];
	}
}

} // namespace duckdb
