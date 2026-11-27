//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/shared_child_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {
class BlockHandle;
class BufferManager;

//! NULL-free fixed-size entries (the child of large lists) in one buffer-managed block, shared by vectors and by the
//! ColumnDataCollections of a recursive CTE. The block spills while no vector pins it. Entries up to the claimed
//! count are written once and never change. The storage may grow at its end: a claim reserves entries behind every
//! published view. Storage never holds NULLs, so it has no validity, children with NULLs are copied as before.
class SharedChildStorage {
public:
	//! Allocate storage for 'capacity' entries of 'type_size' bytes, nothing claimed yet
	static shared_ptr<SharedChildStorage> Create(BufferManager &buffer_manager, idx_t type_size, idx_t capacity,
	                                             BufferHandle &pin);

	SharedChildStorage(BufferManager &buffer_manager, shared_ptr<BlockHandle> block, idx_t type_size, idx_t capacity);

	//! Pin the block for reading or writing
	BufferHandle Pin();
	//! The entries the storage can hold
	idx_t Capacity() const {
		return capacity;
	}
	idx_t TypeSize() const {
		return type_size;
	}
	//! The end of the claimed entries. A claim advances it before the claimant writes the claimed region, and only
	//! views created after those writes cover it, so every view reads written data
	idx_t GetClaimedCount() const {
		return claimed_count.load();
	}
	//! Claim 'extra' entries behind 'current_end'. At most one claimant succeeds for a given end position, so claimed
	//! regions never overlap
	bool TryClaim(idx_t current_end, idx_t extra);
	//! Whether 'other' backs this storage
	bool IsBackedBy(const BufferManager &other) const {
		return &buffer_manager == &other;
	}
	//! The bytes of the block
	idx_t AllocationSize() const;
	//! A process-unique identity, so accounting never confuses storage that reuses an address
	idx_t Id() const {
		return id;
	}

private:
	BufferManager &buffer_manager;
	shared_ptr<BlockHandle> block;
	idx_t type_size;
	idx_t capacity;
	atomic<idx_t> claimed_count;
	idx_t id;
};

//! A flat vector buffer viewing 'count' entries of shared storage from 'offset'. The view pins the block while it is
//! attached. Writes never reach the storage:
//! - a data write (mutable data, SetValue of a value, copies into the buffer, reserve) copies the viewed entries into
//!   private memory and releases the block and its pin, after which the buffer is a plain StandardVectorBuffer
//! - the validity is the view's own, initially all-valid without a mask (the storage holds no NULLs), a NULL write
//!   allocates a private mask, and forwarding the validity elsewhere forwards that mask, never storage memory
//! - slices are views themselves
//! Pointers taken from a view (GetData, UnifiedVectorFormat) stay valid only until the next data write to it, which
//! moves the entries and releases the block.
//! A view counts the storage in its allocation size only if it created it (a list_concat result), all other views
//! borrow it and count nothing, like slices of a StandardVectorBuffer
class SharedChildBuffer : public StandardVectorBuffer {
public:
	SharedChildBuffer(shared_ptr<SharedChildStorage> storage, BufferHandle pin, count_t count, idx_t offset = 0,
	                  bool owns_storage = false);

	//! The storage this buffer presents an unchanged prefix of (attached, flat, from the start, no NULL written), or
	//! nullptr
	const shared_ptr<SharedChildStorage> &GetUnchangedStorage() const;

	data_ptr_t GetDataMutable() override;
	void ResetCapacity(idx_t capacity) override;
	void SetValue(const LogicalType &type, idx_t index, const Value &val) override;
	idx_t GetAllocationSize() const override;

protected:
	using StandardVectorBuffer::SliceInternal;
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, idx_t offset, idx_t end) override;
	buffer_ptr<VectorBuffer> ConstantSliceInternal(const LogicalType &type, count_t count) override;
	void CopyInternal(const Vector &source, const SelectionVector &source_sel, idx_t source_count, idx_t source_offset,
	                  idx_t target_offset, idx_t copy_count) override;
	void ReserveInternal(idx_t new_size) override;

private:
	//! Copy the viewed entries into private memory and release the storage
	void DetachData(idx_t new_capacity);
	//! Release the storage and its pin
	void Release();

	shared_ptr<SharedChildStorage> storage;
	BufferHandle pin;
	//! The first viewed entry of the storage
	idx_t view_offset;
	//! Whether this view created the storage and accounts it
	bool owns_storage;
};

//! One list whose entries are appended behind a prefix
struct SharedChildExtra {
	const Vector &child;
	list_entry_t entry;
};

//! Access to list children backed by SharedChildStorage
struct SharedChildVector {
	//! The storage 'child' presents an unchanged prefix of, or nullptr
	DUCKDB_API static shared_ptr<SharedChildStorage> GetUnchangedStorage(const Vector &child);
	//! Make 'result' a view of the first 'count' entries of 'storage'
	DUCKDB_API static void SetView(Vector &result, shared_ptr<SharedChildStorage> storage, idx_t count);
	//! Copy 'count' entries of 'source' (selected by 'sel' from 'source_offset') into new storage of exactly 'count'
	//! entries. Returns nullptr if any selected entry is NULL
	DUCKDB_API static shared_ptr<SharedChildStorage> Materialize(BufferManager &buffer_manager, const Vector &source,
	                                                             const SelectionVector &sel, idx_t source_offset,
	                                                             idx_t count);
	//! Extend the list 'prefix' of 'prefix_child', an unchanged view of shared storage, by the NULL-free 'extras'.
	//! Writes the extras in place behind the prefix if no other extension claimed that region yet, otherwise copies
	//! the prefix once into new storage with room to grow. 'result_child' becomes a view and 'result_offset' the
	//! offset of the extended list in it. Returns false if the inputs do not fit (no shared prefix, a NULL extra, or
	//! a result that fits a vector), callers then concatenate the regular way.
	//! Limitation: the first extension of a prefix claims the region behind it, whether or not its result is kept.
	//! Any other single-row list_concat on the same prefix copies, as without sharing. In a recursive CTE that is a
	//! second list_concat in the recursive step (e.g. in its WHERE clause or a CASE), or one in the outer query
	//! (... SELECT list_append(acc, 0) FROM r), which sees each iteration's rows before the next iteration runs and
	//! then takes the region first, so the recursion copies every iteration. Results stay correct
	DUCKDB_API static bool TryExtend(BufferManager &buffer_manager, const Vector &prefix_child,
	                                 const list_entry_t &prefix, const vector<SharedChildExtra> &extras,
	                                 Vector &result_child, idx_t &result_offset);
};

} // namespace duckdb
