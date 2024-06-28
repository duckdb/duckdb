#pragma once

#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/common/error_data.hpp"

namespace duckdb {

class QueryResult;

class QueryResultChunkScanState : public ChunkScanState {
public:
	explicit QueryResultChunkScanState(QueryResult &result);
	~QueryResultChunkScanState() override;

public:
	bool LoadNextChunk(ErrorData &error) override;
	bool HasError() const override;
	ErrorData &GetError() override;
	const vector<LogicalType> &Types() const override;
	const vector<string> &Names() const override;

private:
	bool InternalLoad(ErrorData &error);

private:
	QueryResult &result;
};

} // namespace duckdb
