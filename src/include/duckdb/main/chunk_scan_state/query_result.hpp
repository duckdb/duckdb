#pragma once

#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/common/preserved_error.hpp"

namespace duckdb {

class QueryResult;

class QueryResultChunkScanState : public ChunkScanState {
public:
	QueryResultChunkScanState(QueryResult &result);
	~QueryResultChunkScanState();

public:
	bool LoadNextChunk(PreservedError &error) override;
	bool HasError() const override;
	PreservedError &GetError() override;
	const vector<LogicalType> &Types() const override;
	const vector<string> &Names() const override;

private:
	bool InternalLoad(PreservedError &error);

private:
	QueryResult &result;
};

} // namespace duckdb
