//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// storage/index.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <unordered_set>
#include <vector>

#include "common/types/data_chunk.hpp"
#include "common/types/tuple.hpp"

namespace duckdb {

class ClientContext;

//! The index is an abstract base class that serves as the basis for indexes
class Index {
  public:
	virtual ~Index() {
	}

	//! Called when data is appended to the index
	virtual void Append(ClientContext &context, DataChunk &entries,
	                    size_t row_identifier_start) = 0;
	//! Called when data inside the index is updated
	virtual void Update(ClientContext &context,
	                    std::vector<column_t> &column_ids,
	                    DataChunk &update_data, Vector &row_identifiers) = 0;

	// FIXME: what about delete?
};

} // namespace duckdb
