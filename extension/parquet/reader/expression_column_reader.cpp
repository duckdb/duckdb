#include "reader/expression_column_reader.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Expression Column Reader
//===--------------------------------------------------------------------===//
ExpressionColumnReader::ExpressionColumnReader(ClientContext &context, unique_ptr<ColumnReader> child_reader_p,
                                               unique_ptr<Expression> expr_p)
    : ColumnReader(child_reader_p->Reader(), expr_p->return_type, child_reader_p->Schema(), child_reader_p->FileIdx(),
                   child_reader_p->MaxDefine(), child_reader_p->MaxRepeat()),
      child_reader(std::move(child_reader_p)), expr(std::move(expr_p)), executor(context, expr.get()) {
	vector<LogicalType> intermediate_types {child_reader->Type()};
	intermediate_chunk.Initialize(reader.allocator, intermediate_types);
}

unique_ptr<BaseStatistics> ExpressionColumnReader::Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns) {
	// expression stats is not supported (yet)
	return nullptr;
}

void ExpressionColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                            TProtocol &protocol_p) {
	child_reader->InitializeRead(row_group_idx_p, columns, protocol_p);
}

idx_t ExpressionColumnReader::Read(uint64_t num_values, parquet_filter_t &filter, data_ptr_t define_out,
                                   data_ptr_t repeat_out, Vector &result) {
	intermediate_chunk.Reset();
	auto &intermediate_vector = intermediate_chunk.data[0];

	auto amount = child_reader->Read(num_values, filter, define_out, repeat_out, intermediate_vector);
	if (!filter.all()) {
		// work-around for filters: set all values that are filtered to NULL to prevent the cast from failing on
		// uninitialized data
		intermediate_vector.Flatten(amount);
		auto &validity = FlatVector::Validity(intermediate_vector);
		for (idx_t i = 0; i < amount; i++) {
			if (!filter[i]) {
				validity.SetInvalid(i);
			}
		}
	}
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
