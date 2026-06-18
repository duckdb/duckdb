#include "reader/expression_column_reader.hpp"

#include <utility>

#include "parquet_reader.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

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

//===--------------------------------------------------------------------===//
// Expression Column Reader
//===--------------------------------------------------------------------===//
ExpressionColumnReader::ExpressionColumnReader(ClientContext &context, vector<unique_ptr<ColumnReader>> child_readers_p,
                                               unique_ptr<Expression> expr_p, const ParquetColumnSchema &schema_p)
    : ColumnReader(child_readers_p[0]->Reader(), schema_p), child_readers(std::move(child_readers_p)),
      expr(std::move(expr_p)), executor(context, expr.get()) {
	if (child_readers.empty()) {
		throw InternalException("Can't instantiate an ExpressionColumnReader with 0 children");
	}
	InitializeChunk();
}

ExpressionColumnReader::ExpressionColumnReader(ClientContext &context, vector<unique_ptr<ColumnReader>> child_readers_p,
                                               unique_ptr<Expression> expr_p,
                                               unique_ptr<ParquetColumnSchema> owned_schema_p)
    : ColumnReader(child_readers_p[0]->Reader(), *owned_schema_p), child_readers(std::move(child_readers_p)),
      expr(std::move(expr_p)), executor(context, expr.get()), owned_schema(std::move(owned_schema_p)) {
	if (child_readers.empty()) {
		throw InternalException("Can't instantiate an ExpressionColumnReader with 0 children");
	}
	InitializeChunk();
}

void ExpressionColumnReader::InitializeChunk() {
	vector<LogicalType> intermediate_types;
	for (auto &child_reader : child_readers) {
		intermediate_types.push_back(child_reader->Type());
	};
	intermediate_chunk.Initialize(reader.allocator, intermediate_types);
}

void ExpressionColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                            TProtocol &protocol_p) {
	for (auto &child_reader : child_readers) {
		child_reader->InitializeRead(row_group_idx_p, columns, protocol_p);
	}
}

idx_t ExpressionColumnReader::Read(ColumnReaderInput &input, Vector &result) {
	intermediate_chunk.Reset();

	optional_idx amount;
	for (idx_t i = 0; i < child_readers.size(); i++) {
		auto &intermediate_vector = intermediate_chunk.data[i];
		auto &child_reader = child_readers[i];

		auto res = child_reader->Read(input, intermediate_vector);
		if (amount.IsValid() && res != amount.GetIndex()) {
			throw InternalException("ExpressionColumnReader children Read calls produced differing amounts (%d and %d)",
			                        res, amount.GetIndex());
		}
		amount = res;
	}

	// Execute the expression
	intermediate_chunk.SetChildCardinality(amount.GetIndex());
	executor.ExecuteExpression(intermediate_chunk, result);
	return amount.GetIndex();
}

void ExpressionColumnReader::Skip(idx_t num_values) {
	for (auto &child_reader : child_readers) {
		child_reader->Skip(num_values);
	}
}

idx_t ExpressionColumnReader::GroupRowsAvailable() {
	return child_readers[0]->GroupRowsAvailable();
}

} // namespace duckdb
