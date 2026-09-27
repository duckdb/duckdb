//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/chunk_scan_state/query_result_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/chunk_scan_state.hpp"
#include "duckdb/common/error_data.hpp"

namespace duckdb {

class QueryResultStream;

class QueryResultStreamChunkScanState : public ChunkScanState {
public:
	explicit QueryResultStreamChunkScanState(QueryResultStream &stream);
	~QueryResultStreamChunkScanState() override;

public:
	bool LoadNextChunk(ErrorData &error) override;
	bool HasError() const override;
	ErrorData &GetError() override;
	const vector<LogicalType> &Types() const override;
	const vector<Identifier> &Names() const override;

private:
	QueryResultStream &stream;
	ErrorData error;
	bool has_error = false;
};

} // namespace duckdb
