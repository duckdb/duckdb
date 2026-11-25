//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/variant_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class VariantNode;
struct VariantWriterState;

//! Specialization of VectorWriter for VectorVariantType.
//! The rows are collected in a single VariantBuilder and written to the vector when the writer is destroyed. The
//! keys, children and values of the rows are appended to the ones already in the vector.
template <>
struct VectorWriter<VectorVariantType> {
	DUCKDB_API VectorWriter(Vector &vector, idx_t count, idx_t offset);
	DUCKDB_API VectorWriter(VectorWriter &&other) noexcept;
	DUCKDB_API ~VectorWriter();

	//! Write a SQL NULL
	DUCKDB_API void WriteNull();
	//! Write a copy of the value 'node' points at. A NULL or missing node is written as a VARIANT null value.
	DUCKDB_API void WriteValue(const VariantNode &node);

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
	VariantWriterState &GetState();
	void Finalize();

private:
	Vector &variant;
	unique_ptr<VariantWriterState> state;
	idx_t start_idx;
	idx_t count;
	idx_t current_idx;
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
	      list_vec(vector), count(offset + count), current_idx(offset), child_offset(VectorWriterGetListSize(vector)),
	      child_writer(VectorWriterGetListChild(vector), 0, child_offset) {
	}
	VectorWriter(VectorWriter &&other) noexcept
	    : list_data(other.list_data), validity(other.validity), list_vec(other.list_vec), count(other.count),
	      current_idx(other.current_idx), child_offset(other.child_offset),
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
		VectorWriterReserveList(list_vec, child_offset + n);
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

	list_entry_t *list_data;
	ValidityMask &validity;
	Vector &list_vec;
	idx_t count;
	idx_t current_idx;
	idx_t child_offset;
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
		VectorWriterReserveList(parent.list_vec, parent.child_offset + length);
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
