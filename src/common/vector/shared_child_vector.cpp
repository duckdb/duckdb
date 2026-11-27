#include "duckdb/common/vector/shared_child_vector.hpp"

#include "duckdb/common/vector/unified_vector_format.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

shared_ptr<SharedChildStorage> SharedChildStorage::Create(BufferManager &buffer_manager, idx_t type_size,
                                                          idx_t capacity, BufferHandle &pin) {
	D_ASSERT(type_size > 0);
	pin = buffer_manager.Allocate(MemoryTag::COLUMN_DATA, AlignValue(type_size * capacity), false);
	return make_shared_ptr<SharedChildStorage>(buffer_manager, pin.GetBlockHandle(), type_size, capacity);
}

SharedChildStorage::SharedChildStorage(BufferManager &buffer_manager_p, shared_ptr<BlockHandle> block_p,
                                       idx_t type_size_p, idx_t capacity_p)
    : buffer_manager(buffer_manager_p), block(std::move(block_p)), type_size(type_size_p), capacity(capacity_p),
      claimed_count(0) {
	static atomic<idx_t> next_id {0};
	id = next_id++;
}

BufferHandle SharedChildStorage::Pin() {
	return buffer_manager.Pin(block);
}

bool SharedChildStorage::TryClaim(idx_t current_end, idx_t extra) {
	if (current_end + extra > capacity) {
		return false;
	}
	auto expected = current_end;
	return claimed_count.compare_exchange_strong(expected, current_end + extra);
}

idx_t SharedChildStorage::AllocationSize() const {
	return AlignValue(type_size * capacity);
}

SharedChildBuffer::SharedChildBuffer(shared_ptr<SharedChildStorage> storage_p, BufferHandle pin_p, count_t count,
                                     idx_t offset, bool owns_storage_p)
    : StandardVectorBuffer(pin_p.GetDataMutable() + offset * storage_p->TypeSize(), count, storage_p->TypeSize()),
      storage(std::move(storage_p)), pin(std::move(pin_p)), view_offset(offset), owns_storage(owns_storage_p) {
	D_ASSERT(storage->GetClaimedCount() >= offset + count);
	buffer_type = VectorBufferType::SHARED_DATA_BUFFER;
}

const shared_ptr<SharedChildStorage> &SharedChildBuffer::GetUnchangedStorage() const {
	static const shared_ptr<SharedChildStorage> NO_STORAGE;
	// the storage holds no NULLs, a view presents it as long as no NULL was written to the view
	if (!storage || vector_type != VectorType::FLAT_VECTOR || view_offset != 0 || !validity.CannotHaveNull()) {
		return NO_STORAGE;
	}
	return storage;
}

void SharedChildBuffer::Release() {
	storage.reset();
	pin = BufferHandle();
	owns_storage = false;
	buffer_type = VectorBufferType::STANDARD_BUFFER;
}

void SharedChildBuffer::DetachData(idx_t new_capacity) {
	D_ASSERT(storage);
	if (new_capacity > DConstants::MAX_VECTOR_SIZE / type_size) {
		throw OutOfRangeException("Vector capacity is too large");
	}
	auto private_data = Allocator::DefaultAllocator().Allocate(new_capacity * type_size);
	memcpy(private_data.get(), data_ptr, MinValue(capacity, new_capacity) * type_size);
	allocated_data = std::move(private_data);
	data_ptr = allocated_data.get();
	if (new_capacity != capacity) {
		validity.Resize(new_capacity);
	}
	capacity = new_capacity;
	Release();
}

data_ptr_t SharedChildBuffer::GetDataMutable() {
	if (storage) {
		DetachData(capacity);
	}
	return StandardVectorBuffer::GetDataMutable();
}

void SharedChildBuffer::ResetCapacity(idx_t capacity_p) {
	if (storage) {
		// the viewed entries are discarded, the buffer continues as a StandardVectorBuffer that allocates on use
		Release();
		data_ptr = nullptr;
		allocated_data = AllocatedData(Allocator::DefaultAllocator());
	}
	StandardVectorBuffer::ResetCapacity(capacity_p);
}

void SharedChildBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	if (storage && !val.IsNull()) {
		DetachData(capacity);
	}
	// a NULL only writes the view's own validity
	StandardVectorBuffer::SetValue(type, index, val);
}

idx_t SharedChildBuffer::GetAllocationSize() const {
	auto size = StandardVectorBuffer::GetAllocationSize();
	if (storage && owns_storage) {
		size += storage->AllocationSize();
	}
	return size;
}

buffer_ptr<VectorBuffer> SharedChildBuffer::SliceInternal(const LogicalType &type, idx_t offset, idx_t end) {
	if (!storage) {
		return StandardVectorBuffer::SliceInternal(type, offset, end);
	}
	D_ASSERT(end <= capacity);
	const auto count = end - offset;
	auto result = make_buffer<SharedChildBuffer>(storage, storage->Pin(), count_t(count), view_offset + offset);
	// the slice reads the view's validity, including NULLs written to the view
	result->validity.Slice(validity, offset, count);
	return std::move(result);
}

buffer_ptr<VectorBuffer> SharedChildBuffer::ConstantSliceInternal(const LogicalType &type, count_t count) {
	if (!storage) {
		return StandardVectorBuffer::ConstantSliceInternal(type, count);
	}
	// a constant aliasing the storage could be written, copy the single entry instead
	return ConstantReferenceInternal(type, 0, count);
}

void SharedChildBuffer::CopyInternal(const Vector &source, const SelectionVector &source_sel, idx_t source_count,
                                     idx_t source_offset, idx_t target_offset, idx_t copy_count) {
	if (storage) {
		DetachData(capacity);
	}
	StandardVectorBuffer::CopyInternal(source, source_sel, source_count, source_offset, target_offset, copy_count);
}

void SharedChildBuffer::ReserveInternal(idx_t new_size) {
	if (storage) {
		DetachData(new_size);
		return;
	}
	StandardVectorBuffer::ReserveInternal(new_size);
}

shared_ptr<SharedChildStorage> SharedChildVector::GetUnchangedStorage(const Vector &child) {
	if (child.GetVectorType() != VectorType::FLAT_VECTOR) {
		return nullptr;
	}
	auto &buffer = child.GetBufferRef();
	if (!buffer || buffer->GetBufferType() != VectorBufferType::SHARED_DATA_BUFFER) {
		return nullptr;
	}
	return buffer->Cast<SharedChildBuffer>().GetUnchangedStorage();
}

void SharedChildVector::SetView(Vector &result, shared_ptr<SharedChildStorage> storage, idx_t count) {
	auto pin = storage->Pin();
	result.SetBuffer(make_buffer<SharedChildBuffer>(std::move(storage), std::move(pin), count_t(count)));
}

//! Whether any of 'count' entries of 'source' from 'offset' (selected by 'sel') is NULL
static bool HasNull(const Vector &source, const SelectionVector &sel, idx_t offset, idx_t count) {
	UnifiedVectorFormat format;
	source.ToUnifiedFormat(format);
	if (format.validity.CannotHaveNull()) {
		return false;
	}
	for (idx_t i = 0; i < count; i++) {
		if (!format.validity.RowIsValid(format.sel->get_index(sel.get_index(offset + i)))) {
			return true;
		}
	}
	return false;
}

//! Copy the values of 'count' NULL-free entries of 'source' into 'pin' from 'target_offset'
static void WriteEntries(SharedChildStorage &storage, BufferHandle &pin, const Vector &source,
                         const SelectionVector &sel, idx_t source_offset, idx_t target_offset, idx_t count) {
	Vector target(source.GetType(), pin.GetDataMutable() + target_offset * storage.TypeSize(), count);
	VectorOperations::Copy(source, target, sel, source_offset + count, source_offset, 0, count);
}

shared_ptr<SharedChildStorage> SharedChildVector::Materialize(BufferManager &buffer_manager, const Vector &source,
                                                              const SelectionVector &sel, idx_t source_offset,
                                                              idx_t count) {
	if (HasNull(source, sel, source_offset, count)) {
		return nullptr;
	}
	BufferHandle pin;
	auto storage =
	    SharedChildStorage::Create(buffer_manager, GetTypeIdSize(source.GetType().InternalType()), count, pin);
	WriteEntries(*storage, pin, source, sel, source_offset, 0, count);
	auto claimed = storage->TryClaim(0, count);
	D_ASSERT(claimed);
	(void)claimed;
	return storage;
}

bool SharedChildVector::TryExtend(BufferManager &buffer_manager, const Vector &prefix_child, const list_entry_t &prefix,
                                  const vector<SharedChildExtra> &extras, Vector &result_child, idx_t &result_offset) {
	auto storage = GetUnchangedStorage(prefix_child);
	if (!storage || !storage->IsBackedBy(buffer_manager)) {
		return false;
	}
	auto &incremental = *FlatVector::IncrementalSelectionVector();
	idx_t extra_count = 0;
	for (auto &extra : extras) {
		if (extra.child.GetType().InternalType() != prefix_child.GetType().InternalType() ||
		    HasNull(extra.child, incremental, extra.entry.offset, extra.entry.length)) {
			return false;
		}
		extra_count += extra.entry.length;
	}
	const auto prefix_end = prefix.offset + prefix.length;
	const auto required = prefix.length + extra_count;

	shared_ptr<SharedChildStorage> target;
	BufferHandle pin;
	idx_t write_offset;
	bool owns_storage;
	if (storage->TryClaim(prefix_end, extra_count)) {
		target = std::move(storage);
		pin = target->Pin();
		write_offset = prefix_end;
		result_offset = prefix.offset;
		owns_storage = false;
	} else {
		if (required <= STANDARD_VECTOR_SIZE) {
			// a list that fits a vector is cheaper to concatenate the regular way
			return false;
		}
		// copy the prefix once into storage with room to grow, so appends stay amortized constant
		target = SharedChildStorage::Create(buffer_manager, storage->TypeSize(), NextPowerOfTwo(required + 1), pin);
		auto claimed = target->TryClaim(0, required);
		D_ASSERT(claimed);
		(void)claimed;
		WriteEntries(*target, pin, prefix_child, incremental, prefix.offset, 0, prefix.length);
		write_offset = prefix.length;
		result_offset = 0;
		owns_storage = true;
	}
	for (auto &extra : extras) {
		WriteEntries(*target, pin, extra.child, incremental, extra.entry.offset, write_offset, extra.entry.length);
		write_offset += extra.entry.length;
	}
	result_child.SetBuffer(make_buffer<SharedChildBuffer>(std::move(target), std::move(pin),
	                                                      count_t(result_offset + required), 0, owns_storage));
	return true;
}

} // namespace duckdb
