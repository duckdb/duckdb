#include "duckdb/common/types/variant_writer.hpp"
#include "duckdb/common/types/variant/variant_builder.hpp"

namespace duckdb {

//! A row written to a VectorWriter<VectorVariantType>, the entries are relative to the start of the builder
struct VariantWriterRow {
	list_entry_t keys;
	list_entry_t children;
	list_entry_t values;
	//! the encoded value, owned by the heap of the data vector
	string_t blob;
	bool is_null;
};

struct VariantWriterState {
	VariantWriterState() : keys_arena(Allocator::DefaultAllocator()), builder(dictionary, keys_arena) {
	}

	//! owns the keys of the dictionary, they are copied into the keys vector when the rows are written
	ArenaAllocator keys_arena;
	OrderedOwningStringMap<uint32_t> dictionary;
	VariantBuilder builder;
	vector<VariantWriterRow> rows;
};

namespace {

//! Primitives are emitted directly, only containers need the traversal of EmitIterator
void EmitVariantNode(const VariantNode &node, VariantBuilder &builder) {
	if (!node.IsNull() && !node.IsMissing()) {
		const auto type = node.GetTypeId();
		if (type != VariantLogicalType::OBJECT && type != VariantLogicalType::ARRAY) {
			builder.EmitPrimitiveNode(node, type);
			return;
		}
	}
	EmitIterator(node, builder);
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

VariantWriterState &VectorWriter<VectorVariantType>::GetState() {
	if (!state) {
		state = make_uniq<VariantWriterState>();
	}
	return *state;
}

void VectorWriter<VectorVariantType>::WriteNull() {
	D_ASSERT(current_idx < count);
	auto &writer_state = GetState();
	auto &builder = writer_state.builder;
	VariantWriterRow row;
	row.keys = list_entry_t(builder.key_slots.size(), 0);
	row.children = list_entry_t(builder.child_value_ids.size(), 0);
	row.values = list_entry_t(builder.type_ids.size(), 0);
	row.blob = string_t(0U);
	row.is_null = true;
	writer_state.rows.push_back(row);
	current_idx++;
}

void VectorWriter<VectorVariantType>::WriteValue(const VariantNode &node) {
	D_ASSERT(current_idx < count);
	auto &writer_state = GetState();
	auto &builder = writer_state.builder;
	builder.BeginRow();
	EmitVariantNode(node, builder);
	if (builder.blob.size() > NumericLimits<uint32_t>::Maximum()) {
		throw InvalidInputException(
		    "Cannot convert value to VARIANT: encoded row size exceeds the maximum supported %u bytes",
		    NumericLimits<uint32_t>::Maximum());
	}
	VariantWriterRow row;
	row.keys = list_entry_t(builder.row_keys, builder.LocalKey());
	row.children = list_entry_t(builder.row_children, builder.LocalChild());
	row.values = list_entry_t(builder.row_values, builder.LocalValue());
	row.blob = StringVector::AddStringOrBlob(VariantVector::GetData(variant),
	                                         string_t(builder.blob.data(), NumericCast<uint32_t>(builder.blob.size())));
	row.is_null = false;
	writer_state.rows.push_back(row);
	current_idx++;
}

void VectorWriter<VectorVariantType>::Finalize() {
	auto &rows = state->rows;
	auto &builder = state->builder;
	D_ASSERT(rows.size() == current_idx - start_idx);
	if (rows.empty()) {
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

	VariantVectorData variant_data(variant);
	for (idx_t i = 0; i < rows.size(); i++) {
		auto &row = rows[i];
		const auto row_idx = start_idx + i;
		variant_data.keys_data[row_idx] = list_entry_t(keys_offset + row.keys.offset, row.keys.length);
		variant_data.children_data[row_idx] = list_entry_t(children_offset + row.children.offset, row.children.length);
		variant_data.values_data[row_idx] = list_entry_t(values_offset + row.values.offset, row.values.length);
		variant_data.blob_data[row_idx] = row.blob;
		if (row.is_null) {
			FlatVector::SetNull(variant, row_idx, true);
		}
	}

	//! values
	if (total_values) {
		memcpy(variant_data.type_ids_data + values_offset, builder.type_ids.data(), total_values * sizeof(uint8_t));
		memcpy(variant_data.byte_offset_data + values_offset, builder.byte_offsets.data(),
		       total_values * sizeof(uint32_t));
	}

	//! children
	for (idx_t i = 0; i < total_children; i++) {
		const auto child_idx = children_offset + i;
		variant_data.values_index_data[child_idx] = builder.child_value_ids[i];
		if (builder.child_key_ids[i] == VARIANT_INVALID_KEY) {
			variant_data.keys_index_validity.SetInvalid(child_idx);
		} else {
			variant_data.keys_index_data[child_idx] = builder.child_key_ids[i];
		}
	}

	//! keys: copy each distinct key into the keys vector once, then point every key slot at its copy
	vector<string_t> dictionary_keys(state->dictionary.size());
	for (auto &entry : state->dictionary) {
		dictionary_keys[entry.second] = StringVector::AddStringOrBlob(keys_entry, entry.first);
	}
	auto keys_entry_data = FlatVector::GetDataMutable<string_t>(keys_entry);
	for (idx_t i = 0; i < total_keys; i++) {
		keys_entry_data[keys_offset + i] = dictionary_keys[builder.key_slots[i]];
	}
}

} // namespace duckdb
