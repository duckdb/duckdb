//===----------------------------------------------------------------------===//
//                         DuckDB
//
// reader/expression_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>

#include "column_reader.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"
#include "parquet_column_schema.hpp"

namespace duckdb_apache {
namespace thrift {
namespace protocol {
class TProtocol;
} // namespace protocol
} // namespace thrift
} // namespace duckdb_apache
namespace duckdb_parquet {
class ColumnChunk;
} // namespace duckdb_parquet

namespace duckdb {
class ClientContext;
class ThriftFileTransport;
class Vector;

//! A column reader that executes an expression over a child reader
class ExpressionColumnReader : public ColumnReader {
public:
	static constexpr const PhysicalType TYPE = PhysicalType::INVALID;

public:
	ExpressionColumnReader(ClientContext &context, vector<unique_ptr<ColumnReader>> child_readers,
	                       unique_ptr<Expression> expr, const ParquetColumnSchema &schema);
	ExpressionColumnReader(ClientContext &context, vector<unique_ptr<ColumnReader>> child_readers,
	                       unique_ptr<Expression> expr, unique_ptr<ParquetColumnSchema> owned_schema);

	//! Reader(s) to produce the input(s) for the expression
	vector<unique_ptr<ColumnReader>> child_readers;
	DataChunk intermediate_chunk;
	unique_ptr<Expression> expr;
	ExpressionExecutor executor;

	// If this reader was created on top of a child reader, after-the-fact, the schema needs to live somewhere
	unique_ptr<ParquetColumnSchema> owned_schema;

public:
	void InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns, TProtocol &protocol_p) override;

	idx_t Read(ColumnReaderInput &input, Vector &result) override;

	void Skip(idx_t num_values) override;
	idx_t GroupRowsAvailable() override;

	uint64_t TotalCompressedSize() override {
		idx_t total_compressed_size = 0;
		for (auto &child_reader : child_readers) {
			total_compressed_size += child_reader->TotalCompressedSize();
		}
		return total_compressed_size;
	}

	idx_t FileOffset() const override {
		return child_readers[0]->FileOffset();
	}

	void RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) override {
		for (auto &child_reader : child_readers) {
			child_reader->RegisterPrefetch(transport, allow_merge);
		}
	}

private:
	void InitializeChunk();
};

} // namespace duckdb
