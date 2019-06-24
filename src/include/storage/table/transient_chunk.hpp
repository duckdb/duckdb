//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/transient_chunk.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/table/version_chunk.hpp"

namespace duckdb {

class TransientChunk : public VersionChunk {
public:
    TransientChunk(DataTable &table, index_t start);

	//! The string heap of the storage chunk
	StringHeap string_heap;

};

} // namespace duckdb

