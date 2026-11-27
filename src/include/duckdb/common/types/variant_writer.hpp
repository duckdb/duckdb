//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/variant_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/types/variant/variant_builder.hpp"
#include "duckdb/common/vector/list_vector.hpp"

namespace duckdb {

//! The state shared by the VARIANT writers of a vector. It is kept with the string heap of the keys vector, so every
//! writer of the vector (e.g. one per list for LIST(STRUCT(VARIANT))) reuses its allocations and its dictionary.
struct VariantWriterState final : public AuxiliaryDataHolder {
	explicit VariantWriterState(ArenaAllocator &keys_arena);

	idx_t GetAllocationSize() const override;

	//! the keys written so far, allocated in the string heap of the keys vector
	OrderedOwningStringMap<uint32_t> dictionary;
	//! the key of every dictionary entry, at the index of the entry
	vector<string_t> dictionary_keys;
	//! emits one row at a time, its arrays keep their capacity between rows
	VariantBuilder builder;
};

//! Specialization of VectorWriter for VectorVariantType.
//! Every row is emitted into a VariantBuilder and then written to the vector. The keys, children and values of the
//! rows are appended to the ones already in the vector.
template <>
struct VectorWriter<VectorVariantType> {
	DUCKDB_API VectorWriter(Vector &vector, idx_t count, idx_t offset);
	DUCKDB_API VectorWriter(VectorWriter &&other) noexcept;
	DUCKDB_API ~VectorWriter();

	//! Write a SQL NULL
	void WriteNull() {
		D_ASSERT(current_idx < count);
		if (!state) {
			Initialize();
		}
		WriteNullRow();
	}

	//! Write a copy of the value 'node' points at. A NULL or missing node is written as a VARIANT null value.
	void WriteValue(const VariantNode &node) {
		if (node.IsNull() || node.IsMissing()) {
			BeginRow().EmitNull();
			FinishRow();
			return;
		}
		WriteNode(node, VariantNodeStorageAccess::GetStorageType(node));
	}

	//! Write a copy of the value 'node' points at, for a node that is not NULL or missing and whose type is 'type'.
	//! Saves looking up the type again when the caller already has it.
	void WriteValue(const VariantNode &node, LogicalTypeId type) {
		D_ASSERT(!node.IsNull() && !node.IsMissing() && node.GetTypeId() == type);
		WriteNode(node, VariantTypeFromLogicalTypeId(type, type == LogicalTypeId::BOOLEAN && node.GetBool()));
	}

	void Truncate() noexcept {
		count = current_idx;
	}

private:
	friend struct VectorWriter<VectorListType<VectorVariantType>>;
	friend class DynamicListWriter<VectorVariantType>;

	//! Allow 'n' more rows to be written, used by the LIST(VARIANT) writer to write all elements through one writer
	void Extend(idx_t n) {
		count += n;
	}

	void WriteNode(const VariantNode &node, VariantLogicalType type_id) {
		auto &builder = BeginRow();
		if (type_id == VariantLogicalType::OBJECT || type_id == VariantLogicalType::ARRAY) {
			EmitContainer(node, builder);
		} else {
			builder.EmitPrimitiveNode(node, type_id);
		}
		FinishRow();
	}

	VariantBuilder &BeginRow() {
		D_ASSERT(current_idx < count);
		if (!state) {
			Initialize();
		}
		auto &builder = state->builder;
		//! a row that threw while it was emitted leaves its entries behind
		builder.type_ids.clear();
		builder.byte_offsets.clear();
		builder.child_key_ids.clear();
		builder.child_value_ids.clear();
		builder.key_slots.clear();
		builder.BeginRow();
		return builder;
	}

	//! Writes the row the builder just emitted to the vector
	void FinishRow() {
		auto &builder = state->builder;
		const auto blob_size = builder.blob.size();
		if (blob_size > NumericLimits<uint32_t>::Maximum()) {
			ThrowRowTooLarge();
		}
		const auto value_count = builder.type_ids.size();
		const auto child_count = builder.child_value_ids.size();
		//! only objects have keys, so a row without children has no keys either
		if (child_count &&
		    (children_size + child_count > children_capacity || keys_size + builder.key_slots.size() > keys_capacity)) {
			ReserveChildren(child_count, builder.key_slots.size());
		}
		if (values_size + value_count > values_capacity) {
			ReserveValues(values_size + value_count);
		}
		string_t blob(builder.blob.data(), static_cast<uint32_t>(blob_size));
		if (!blob.IsInlined()) {
			blob = (blob_heap ? *blob_heap : InitializeBlobHeap()).AddBlob(blob);
		}

		blob_data[current_idx] = blob;
		values_data[current_idx] = list_entry_t(values_size, value_count);
		if (value_count == 1) {
			type_ids_data[values_size] = builder.type_ids.data()[0];
			byte_offset_data[values_size] = builder.byte_offsets.data()[0];
		} else {
			memcpy(type_ids_data + values_size, builder.type_ids.data(), value_count * sizeof(uint8_t));
			memcpy(byte_offset_data + values_size, builder.byte_offsets.data(), value_count * sizeof(uint32_t));
		}
		values_size += value_count;
		if (child_count) {
			WriteChildren();
		} else {
			children_data[current_idx] = list_entry_t(children_size, 0);
			keys_data[current_idx] = list_entry_t(keys_size, 0);
		}
		current_idx++;
	}

	DUCKDB_API void Initialize();
	//! Reads the row arrays of the vector again, after a parent list moved them
	DUCKDB_API void RefreshRows();
	void RefreshValues();
	void RefreshChildren();
	DUCKDB_API void ReserveValues(idx_t required);
	//! Makes room for 'child_count' more children and 'key_count' more keys
	DUCKDB_API void ReserveChildren(idx_t child_count, idx_t key_count);
	DUCKDB_API void WriteChildren();
	DUCKDB_API void WriteNullRow();
	DUCKDB_API StringHeap &InitializeBlobHeap();
	DUCKDB_API static void ThrowRowTooLarge();
	DUCKDB_API static void EmitContainer(const VariantNode &node, VariantBuilder &builder);

private:
	Vector &variant;
	//! set by the first write, owned by the keys vector
	optional_ptr<VariantWriterState> state;
	idx_t count;
	idx_t current_idx;

	//! rows
	list_entry_t *keys_data = nullptr;
	list_entry_t *children_data = nullptr;
	list_entry_t *values_data = nullptr;
	string_t *blob_data = nullptr;
	optional_ptr<StringHeap> blob_heap;
	//! values
	uint8_t *type_ids_data = nullptr;
	uint32_t *byte_offset_data = nullptr;
	idx_t values_size = 0;
	idx_t values_capacity = 0;
	//! children
	uint32_t *keys_index_data = nullptr;
	uint32_t *values_index_data = nullptr;
	optional_ptr<ValidityMask> keys_index_validity;
	idx_t children_size = 0;
	idx_t children_capacity = 0;
	//! keys
	string_t *keys_entry_data = nullptr;
	idx_t keys_size = 0;
	idx_t keys_capacity = 0;
};

//! Specialization of VectorWriter for VectorListType<VectorVariantType>.
//! The elements of all lists are written through a single VARIANT writer, so they share one VariantBuilder instead
//! of creating one per list.
template <>
struct VectorWriter<VectorListType<VectorVariantType>> {
public:
	//! Range returned by WriteList(n), yields the shared element writer once for each of the n elements
	struct WriteRange {
		class RangeIterator {
		public:
			RangeIterator(VectorWriter<VectorVariantType> &writer, idx_t pos) : writer(writer), pos(pos) {
			}
			VectorWriter<VectorVariantType> &operator*() {
				return writer;
			}
			RangeIterator &operator++() { // NOLINT: match stl API
				++pos;
				return *this;
			}
			bool operator!=(const RangeIterator &other) const {
				return pos != other.pos;
			}

		private:
			VectorWriter<VectorVariantType> &writer;
			idx_t pos;
		};

		WriteRange(VectorWriter<VectorVariantType> &child_writer, idx_t n) : child_writer(child_writer), length(n) {
		}

		RangeIterator begin() { // NOLINT: match stl API
			return RangeIterator(child_writer, 0);
		}
		RangeIterator end() { // NOLINT: match stl API
			return RangeIterator(child_writer, length);
		}

		VectorWriter<VectorVariantType> &child_writer;
		idx_t length;
	};

public:
	VectorWriter(Vector &vector, idx_t count, idx_t offset)
	    : list_data(FlatVector::GetDataMutable<list_entry_t>(vector)), validity(FlatVector::ValidityMutable(vector)),
	      list_vec(vector), count(offset + count), current_idx(offset), child_offset(ListVector::GetListSize(vector)),
	      child_capacity(ListVector::GetListCapacity(vector)),
	      child_writer(ListVector::GetChildMutable(vector), 0, child_offset) {
	}
	VectorWriter(VectorWriter &&other) noexcept
	    : list_data(other.list_data), validity(other.validity), list_vec(other.list_vec), count(other.count),
	      current_idx(other.current_idx), child_offset(other.child_offset), child_capacity(other.child_capacity),
	      child_writer(std::move(other.child_writer)) {
		other.count = other.current_idx;
	}
	~VectorWriter() {
		D_ASSERT(Exception::UncaughtException() || current_idx == count);
	}

	void WriteNull() {
		D_ASSERT(current_idx < count);
		validity.SetInvalid(current_idx);
		current_idx++;
	}

	void Truncate() noexcept {
		count = current_idx;
	}

	//! Reserve n elements, record the list entry for this row, and return a WriteRange over the element writer.
	//! All n elements must be written before the next list is opened.
	WriteRange WriteList(idx_t n) {
		D_ASSERT(current_idx < count);
		// the elements of the previous list must all have been written
		D_ASSERT(child_writer.current_idx == child_offset);
		ReserveElements(child_offset + n);
		VectorWriterSetListSize(list_vec, child_offset + n);
		list_data[current_idx] = {child_offset, n};
		current_idx++;
		child_offset += n;
		child_writer.Extend(n);
		return WriteRange(child_writer, n);
	}

	//! Open a list whose final length is unknown. Each WriteElement() call on the returned writer allows one more
	//! element to be written; the list entry is recorded when it goes out of scope.
	DynamicListWriter<VectorVariantType> WriteDynamicList();

private:
	friend class DynamicListWriter<VectorVariantType>;

	void ReserveElements(idx_t required) {
		if (required <= child_capacity) {
			return;
		}
		VectorWriterReserveList(list_vec, required);
		child_capacity = ListVector::GetListCapacity(list_vec);
		child_writer.RefreshRows();
	}

	list_entry_t *list_data;
	ValidityMask &validity;
	Vector &list_vec;
	idx_t count;
	idx_t current_idx;
	idx_t child_offset;
	idx_t child_capacity;
	//! Writes the elements of all lists, its rows start at the list size of the vector when this writer was created
	VectorWriter<VectorVariantType> child_writer;
};

//! Writer for a single LIST(VARIANT) list whose final length is unknown. Every element is written through the shared
//! element writer of the parent, so there is nothing to rebuild when the list grows.
template <>
class DynamicListWriter<VectorVariantType> {
public:
	DynamicListWriter(VectorWriter<VectorListType<VectorVariantType>> &parent, idx_t row_idx)
	    : parent(parent), row_idx(row_idx), length(0) {
	}
	DynamicListWriter(const DynamicListWriter &) = delete;
	DynamicListWriter(DynamicListWriter &&) = delete;
	~DynamicListWriter() { // NOLINT(bugprone-exception-escape): WriteElement reserved the capacity of the list
		D_ASSERT(Exception::UncaughtException() || parent.child_writer.current_idx == parent.child_offset + length);
		parent.list_data[row_idx] = {parent.child_offset, length};
		parent.child_offset += length;
		VectorWriterSetListSize(parent.list_vec, parent.child_offset);
	}

	//! Returns the element writer, which must be used to write exactly one value before WriteElement() is called again
	VectorWriter<VectorVariantType> &WriteElement() {
		// the previous element must have been written
		D_ASSERT(parent.child_writer.current_idx == parent.child_offset + length);
		length++;
		parent.ReserveElements(parent.child_offset + length);
		parent.child_writer.Extend(1);
		return parent.child_writer;
	}

private:
	VectorWriter<VectorListType<VectorVariantType>> &parent;
	idx_t row_idx;
	idx_t length;
};

inline DynamicListWriter<VectorVariantType> VectorWriter<VectorListType<VectorVariantType>>::WriteDynamicList() {
	D_ASSERT(current_idx < count);
	D_ASSERT(child_writer.current_idx == child_offset);
	const auto row_idx = current_idx;
	current_idx++;
	return DynamicListWriter<VectorVariantType>(*this, row_idx);
}

} // namespace duckdb
