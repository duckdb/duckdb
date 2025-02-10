#include "reader/expression_column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Expression Column Reader
//===--------------------------------------------------------------------===//
ExpressionColumnReader::ExpressionColumnReader(ClientContext &context, unique_ptr<ColumnReader> child_reader_p,
                                               unique_ptr<Expression> expr_p)
    : ColumnReader(child_reader_p->Reader(), child_reader_p->Schema()), child_reader(std::move(child_reader_p)),
      expr(std::move(expr_p)), executor(context, expr.get()) {
	vector<LogicalType> intermediate_types {child_reader->Type()};
	intermediate_chunk.Initialize(reader.allocator, intermediate_types);
}

void ExpressionColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                            TProtocol &protocol_p) {
	child_reader->InitializeRead(row_group_idx_p, columns, protocol_p);
}

idx_t ExpressionColumnReader::Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) {
	intermediate_chunk.Reset();
	auto &intermediate_vector = intermediate_chunk.data[0];

	auto amount = child_reader->Read(num_values, define_out, repeat_out, intermediate_vector);
	// Execute the expression
	intermediate_chunk.SetCardinality(amount);
	executor.ExecuteExpression(intermediate_chunk, result);
	return amount;
}

void ExpressionColumnReader::Skip(idx_t num_values) {
	child_reader->Skip(num_values);
}

idx_t ExpressionColumnReader::GroupRowsAvailable() {
	return child_reader->GroupRowsAvailable();
}

} // namespace duckdb
