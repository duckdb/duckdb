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
ExpressionColumnReader::ExpressionColumnReader(ClientContext &context, unique_ptr<ColumnReader> child_reader_p,
                                               unique_ptr<Expression> expr_p, const ParquetColumnSchema &schema_p)
    : ColumnReader(child_reader_p->Reader(), schema_p), child_reader(std::move(child_reader_p)),
      expr(std::move(expr_p)), executor(context, expr.get()) {
	vector<LogicalType> intermediate_types {child_reader->Type()};
	intermediate_chunk.Initialize(reader.allocator, intermediate_types);
}

ExpressionColumnReader::ExpressionColumnReader(ClientContext &context, unique_ptr<ColumnReader> child_reader_p,
                                               unique_ptr<Expression> expr_p,
                                               unique_ptr<ParquetColumnSchema> owned_schema_p)
    : ColumnReader(child_reader_p->Reader(), *owned_schema_p), child_reader(std::move(child_reader_p)),
      expr(std::move(expr_p)), executor(context, expr.get()), owned_schema(std::move(owned_schema_p)) {
	vector<LogicalType> intermediate_types {child_reader->Type()};
	intermediate_chunk.Initialize(reader.allocator, intermediate_types);
}

void ExpressionColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                            TProtocol &protocol_p) {
	child_reader->InitializeRead(row_group_idx_p, columns, protocol_p);
}

static void ReverseSelectionVector(const SelectionVector &input, SelectionVector &output, idx_t input_count,
                                   idx_t result_count) {
	//! For an input selection vector: [5, 10],
	//! produce a new selection vector: [-, -, -, -, 0, -, -, -, -, 1]
	idx_t result_index = 0;
	idx_t last_index = 0;
	for (idx_t i = 0; i < input_count; i++) {
		idx_t value = input[i];
		last_index = i;
		if (value >= result_count) {
			throw InternalException("Not enough room in the resulting selection vector (%d) to reverse the selection "
			                        "vector, encountered value: %d, at index: %d",
			                        input_count, value, i);
		}
		for (; result_index <= value; result_index++) {
			output[result_index] = last_index;
		}
	}
	//! Fill the remainder of the selection vector, to remove any uninitialized values
	for (; result_index < result_count; result_index++) {
		output[result_index] = last_index;
	}
}

void ExpressionColumnReader::Select(ColumnReaderInput &input, Vector &result, const SelectionVector &sel,
                                    idx_t approved_tuple_count) {
	intermediate_chunk.Reset();
	auto &intermediate_vector = intermediate_chunk.data[0];

	child_reader->Select(input, intermediate_vector, sel, approved_tuple_count);
	intermediate_chunk.SetCardinality(input.num_values);
	//! This executes the expression *and* applies the selection vector in the process
	executor.ExecuteExpression(intermediate_chunk, result, sel, approved_tuple_count);
	if (input.num_values != approved_tuple_count) {
		//! Since the caller expects the rows to be in the spot they would have been in the input chunk
		//! We now have to reverse this selection ..
		SelectionVector inverted_sel(input.num_values);
		ReverseSelectionVector(sel, inverted_sel, approved_tuple_count, input.num_values);
		result.Slice(inverted_sel, input.num_values);
	}
}

idx_t ExpressionColumnReader::Read(ColumnReaderInput &input, Vector &result) {
	intermediate_chunk.Reset();
	auto &intermediate_vector = intermediate_chunk.data[0];

	auto amount = child_reader->Read(input, intermediate_vector);
	// Execute the expression
	intermediate_chunk.SetChildCardinality(amount);
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
