#include "duckdb/common/vector/fsst_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/fsst.hpp"

namespace duckdb {

VectorFSSTStringBuffer::VectorFSSTStringBuffer(idx_t capacity) : VectorStringBuffer(capacity) {
	buffer_type = VectorBufferType::FSST_BUFFER;
	vector_type = VectorType::FSST_VECTOR;
}

void VectorFSSTStringBuffer::SetVectorType(VectorType new_vector_type) {
	throw InternalException("SetVectorType not supported for FSST vector");
}

void VectorFSSTStringBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	D_ASSERT(type.InternalType() == PhysicalType::VARCHAR);
	D_ASSERT(vector_type == VectorType::FSST_VECTOR);
}

buffer_ptr<VectorBuffer> VectorFSSTStringBuffer::Slice(const SelectionVector &sel, idx_t count) {
	// return nullptr to indicate the caller should flatten first
	return nullptr;
}

Value VectorFSSTStringBuffer::GetValue(const LogicalType &type, idx_t index) const {
	if (!validity.RowIsValid(index)) {
		return Value(type);
	}
	auto str_compressed = reinterpret_cast<const string_t *>(data_ptr)[index];
	auto decoder = const_cast<VectorFSSTStringBuffer *>(this)->GetDecoder();
	auto &decompress_buffer = const_cast<VectorFSSTStringBuffer *>(this)->GetDecompressBuffer();
	auto string_val = FSSTPrimitives::DecompressValue(decoder, str_compressed.GetData(), str_compressed.GetSize(),
	                                                  decompress_buffer);
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return Value(std::move(string_val));
	case LogicalTypeId::BLOB:
		return Value::BLOB_RAW(string_val);
	default:
		throw InternalException("Unsupported type for FSST vector GetValue");
	}
}

buffer_ptr<VectorBuffer> VectorFSSTStringBuffer::Flatten(const LogicalType &type, const SelectionVector &sel,
                                                         idx_t count) {
	// even though count may only be a part of the vector, we need to flatten the whole thing due to the way
	// ToUnifiedFormat uses flatten
	idx_t total_count = GetCount();
	// create a non-owning buffer_ptr to construct a temporary source vector
	buffer_ptr<VectorBuffer> non_owning_ref(this, [](VectorBuffer *) {});
	Vector source(type, VectorType::FSST_VECTOR, std::move(non_owning_ref));
	// create vector to decompress into
	Vector result(type, total_count);
	// now copy the data of this vector to the other vector, decompressing the strings in the process
	VectorOperations::Copy(source, result, total_count, 0, 0);
	return result.GetBuffer();
}

VectorFSSTStringBuffer &FSSTVector::GetFSSTBuffer(const Vector &vector) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (vector.GetVectorType() != VectorType::FSST_VECTOR) {
		throw InternalException("FSSTVector::GetFSSTBuffer called on a non-FSST vector");
	}
	if (!vector.buffer || vector.buffer->GetBufferType() != VectorBufferType::FSST_BUFFER) {
		throw InternalException("FSSTVector has a non-FSST buffer");
	}
	return vector.buffer->Cast<VectorFSSTStringBuffer>();
}

StringHeap &FSSTVector::GetStringHeap(const Vector &vector) {
	auto &fsst_buffer = GetFSSTBuffer(vector);
	return fsst_buffer.GetHeap();
}

string_t FSSTVector::AddCompressedString(Vector &vector, const char *data, idx_t len) {
	return FSSTVector::AddCompressedString(vector, string_t(data, UnsafeNumericCast<uint32_t>(len)));
}

string_t FSSTVector::AddCompressedString(Vector &vector, string_t data) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	auto &fsst_heap = GetStringHeap(vector);
	return fsst_heap.AddBlob(data);
}

void *FSSTVector::GetDecoder(const Vector &vector) {
	auto &fsst_string_buffer = GetFSSTBuffer(vector);
	return fsst_string_buffer.GetDecoder();
}

vector<unsigned char> &FSSTVector::GetDecompressBuffer(const Vector &vector) {
	auto &fsst_string_buffer = GetFSSTBuffer(vector);
	return fsst_string_buffer.GetDecompressBuffer();
}

void FSSTVector::Create(Vector &vector, buffer_ptr<void> &duckdb_fsst_decoder, const idx_t string_block_limit,
                        idx_t capacity) {
	vector.buffer = make_buffer<VectorFSSTStringBuffer>(capacity);
	vector.SetVectorType(VectorType::FSST_VECTOR);
	auto &fsst_string_buffer = vector.buffer->Cast<VectorFSSTStringBuffer>();
	fsst_string_buffer.AddDecoder(duckdb_fsst_decoder, string_block_limit);
}

void FSSTVector::SetCount(Vector &vector, idx_t count) {
	auto &fsst_string_buffer = GetFSSTBuffer(vector);
	fsst_string_buffer.SetCount(count);
}

idx_t FSSTVector::GetCount(const Vector &vector) {
	auto &fsst_string_buffer = GetFSSTBuffer(vector);
	return fsst_string_buffer.GetCount();
}

void FSSTVector::DecompressVector(const Vector &src, Vector &dst, idx_t src_offset, idx_t dst_offset, idx_t copy_count,
                                  const SelectionVector *sel) {
	D_ASSERT(src.GetVectorType() == VectorType::FSST_VECTOR);
	D_ASSERT(dst.GetVectorType() == VectorType::FLAT_VECTOR);
	auto dst_mask = FlatVector::Validity(dst);
	auto ldata = FSSTVector::GetCompressedData(src);
	auto decoder = FSSTVector::GetDecoder(src);
	auto tdata = FlatVector::GetDataMutable<string_t>(dst);
	auto &str_allocator = StringVector::GetStringAllocator(dst);
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = sel->get_index(src_offset + i);
		auto target_idx = dst_offset + i;
		string_t compressed_string = ldata[source_idx];
		if (dst_mask.RowIsValid(target_idx) && compressed_string.GetSize() > 0) {
			tdata[target_idx] = FSSTPrimitives::DecompressValue(decoder, str_allocator, compressed_string.GetData(),
			                                                    compressed_string.GetSize());
		} else {
			tdata[target_idx] = string_t(nullptr, 0);
		}
	}
}

} // namespace duckdb
