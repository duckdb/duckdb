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

Value VectorFSSTStringBuffer::GetValue(const LogicalType &type, idx_t index) const {
	if (!validity.RowIsValid(index)) {
		return Value(type);
	}
	auto str_compressed = reinterpret_cast<const string_t *>(data_ptr)[index];
	auto decoder = GetDecoder();
	auto &decompress_buffer = GetDecompressBuffer();
	auto string_val =
	    FSSTPrimitives::DecompressValue(decoder, str_compressed.GetData(), str_compressed.GetSize(), decompress_buffer);
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
                                                         idx_t count) const {
	auto result = make_buffer<VectorStringBuffer>(count);

	auto fsst_data = reinterpret_cast<const string_t *>(data_ptr);
	auto result_data = reinterpret_cast<string_t *>(result->GetData());
	auto &str_allocator = result->GetStringAllocator();
	auto decoder = GetDecoder();
	auto &dst_mask = result->GetValidityMask();
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = sel.get_index(i);
		auto target_idx = i;
		if (!validity.RowIsValid(source_idx)) {
			// NULL value
			dst_mask.SetInvalid(target_idx);
			continue;
		}
		auto &compressed_string = fsst_data[source_idx];
		if (compressed_string.GetSize() > 0) {
			result_data[target_idx] = FSSTPrimitives::DecompressValue(
			    decoder, str_allocator, compressed_string.GetData(), compressed_string.GetSize());
		} else {
			// empty string
			result_data[target_idx] = string_t(nullptr, 0);
		}
	}
	return result;
}

VectorFSSTStringBuffer &FSSTVector::GetFSSTBuffer(const Vector &vector) {
	D_ASSERT(vector.GetType().InternalType() == PhysicalType::VARCHAR);
	if (vector.GetVectorType() != VectorType::FSST_VECTOR) {
		throw InternalException("FSSTVector::GetFSSTBuffer called on a non-FSST vector");
	}
	if (!vector.GetBufferRef() || vector.Buffer().GetBufferType() != VectorBufferType::FSST_BUFFER) {
		throw InternalException("FSSTVector has a non-FSST buffer");
	}
	return vector.GetBufferRef()->Cast<VectorFSSTStringBuffer>();
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
	vector.SetBuffer(make_buffer<VectorFSSTStringBuffer>(capacity));
	auto &fsst_string_buffer = vector.BufferMutable().Cast<VectorFSSTStringBuffer>();
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

} // namespace duckdb
