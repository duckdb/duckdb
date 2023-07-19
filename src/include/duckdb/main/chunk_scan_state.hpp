#pragma once

namespace duckdb {

class QueryResult;
class DataChunk;

//! Abstract chunk fetcher
class ChunkScanState {
public:
	ChunkScanState(idx_t position) {
	}
	virtual ~ChunkScanState() {
	}

public:
	virtual bool LoadNextChunk(PreservedError &error) = 0;
	virtual bool HasError() const = 0;
	virtual PreservedError &GetError() = 0;
	idx_t CurrentOffset() const;
	idx_t RemainingInChunk() const;
	DataChunk &CurrentChunk();
	bool Finished() const;
	bool ScanStarted() const;
	void IncreaseOffset(idx_t increment);

protected:
	idx_t offset = 0;
	bool finished = false;
	unique_ptr<DataChunk> current_chunk;
};

class QueryResultChunkScanState : public ChunkScanState {
public:
	QueryResultChunkScanState(QueryResult &result);
	~QueryResultChunkScanState();

public:
	bool LoadNextChunk(PreservedError &error) override;
	bool HasError() const override;
	PreservedError &GetError() override;

private:
	bool InternalLoad(PreservedError &error);

private:
	QueryResult &result;
};

} // namespace duckdb
