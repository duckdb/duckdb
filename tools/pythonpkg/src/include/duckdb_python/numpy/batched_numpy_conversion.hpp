//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/numpy/batched_numpy_conversion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"

namespace duckdb {
class BufferManager;
class ClientContext;

//!  A BatchedNumpyConversion holds a number of data entries that are partitioned by batch index
//! Scans over a BatchedNumpyConversion are ordered by batch index
class BatchedNumpyConversion {
public:
	DUCKDB_API BatchedNumpyConversion(vector<LogicalType> types);
	~BatchedNumpyConversion();

	//! Appends a datachunk with the given batch index to the batched collection
	DUCKDB_API void Append(DataChunk &input, idx_t batch_index);

	//! Merge the other batched chunk collection into this batched collection
	DUCKDB_API void Merge(BatchedNumpyConversion &other);

	//! Fetch a column data collection from the batched data collection - this consumes all of the data stored within
	DUCKDB_API unique_ptr<NumpyResultConversion> FetchCollection();

	DUCKDB_API string ToString() const;
	DUCKDB_API void Print() const;

private:
	struct CachedCollection {
		idx_t batch_index = DConstants::INVALID_INDEX;
		NumpyResultConversion *collection = nullptr;
	};

	vector<LogicalType> types;
	//! The data of the batched chunk collection - a set of batch_index -> NumpyResultConversion pointers
	map<idx_t, unique_ptr<NumpyResultConversion>> data;
	//! The last batch collection that was inserted into
	CachedCollection last_collection;
};
} // namespace duckdb
