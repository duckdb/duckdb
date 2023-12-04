#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/preserved_error.hpp"

namespace duckdb {

class DataChunk;

//! Abstract chunk fetcher
class ChunkScanState {
public:
	explicit ChunkScanState();
	virtual ~ChunkScanState();

public:
	ChunkScanState(const ChunkScanState &other) = delete;
	ChunkScanState(ChunkScanState &&other) = default;
	ChunkScanState &operator=(const ChunkScanState &other) = delete;
	ChunkScanState &operator=(ChunkScanState &&other) = default;

public:
	virtual bool LoadNextChunk(PreservedError &error) = 0;
	virtual bool HasError() const = 0;
	virtual PreservedError &GetError() = 0;
	virtual const vector<LogicalType> &Types() const = 0;
	virtual const vector<string> &Names() const = 0;
	idx_t CurrentOffset() const;
	idx_t RemainingInChunk() const;
	DataChunk &CurrentChunk();
	bool ChunkIsEmpty() const;
	bool Finished() const;
	bool ScanStarted() const;
	void IncreaseOffset(idx_t increment, bool unsafe = false);

protected:
	idx_t offset = 0;
	bool finished = false;
	unique_ptr<DataChunk> current_chunk;
};

} // namespace duckdb
