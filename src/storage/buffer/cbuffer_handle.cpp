#include "duckdb/storage/buffer/cbuffer_handle.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

bool CBufferHandle::IsValid() const {
	return data != nullptr;
}

data_ptr_t CBufferHandle::Ptr() const {
	return data;
}

data_ptr_t CBufferHandle::Ptr() {
	return data;
}

FileBuffer &CBufferHandle::GetFileBuffer() {
	throw NotImplementedException("CBufferHandle does not have an internal FileBuffer");
}

void CBufferHandle::Destroy() {
	// TODO: Call the Destroy callback?
}

const shared_ptr<BlockHandle> &CBufferHandle::GetBlockHandle() const {
	throw NotImplementedException("CBufferHandle does not have an internal BlockHandle");
}

} // namespace duckdb
