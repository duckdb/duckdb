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

class ChunkFormat;
template <class FORMAT>
class QueryResultStream;

class QueryResultStreamChunkScanState : public ChunkScanState {
public:
	explicit QueryResultStreamChunkScanState(QueryResultStream<ChunkFormat> &stream);
	~QueryResultStreamChunkScanState() override;

public:
	bool LoadNextChunk(ErrorData &error) override;
	bool HasError() const override;
	ErrorData &GetError() override;
	const vector<LogicalType> &Types() const override;
	const vector<Identifier> &Names() const override;

private:
	QueryResultStream<ChunkFormat> &stream;
	ErrorData error;
	bool has_error = false;
};

} // namespace duckdb
