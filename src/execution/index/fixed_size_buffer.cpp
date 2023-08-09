#include "duckdb/execution/index/fixed_size_buffer.hpp"

namespace duckdb {

data_ptr_t FixedSizeBuffer::GetPtr() {
	return memory_ptr;
}

} // namespace duckdb
