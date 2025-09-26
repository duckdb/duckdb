//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/variant_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_writer.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

class VariantColumnWriter : public ColumnWriter {
public:
	VariantColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema, vector<string> schema_path_p,
	                    vector<unique_ptr<ColumnWriter>> child_writers_p, bool can_have_nulls)
	    : ColumnWriter(writer, column_schema, std::move(schema_path_p), can_have_nulls),
	      child_writers(std::move(child_writers_p)) {
	}
	~VariantColumnWriter() override = default;

	vector<unique_ptr<ColumnWriter>> child_writers;

public:
	unique_ptr<ColumnWriterState> InitializeWriteState(duckdb_parquet::RowGroup &row_group) override;
	bool HasAnalyze() override;
	void Analyze(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count) override;
	void FinalizeAnalyze(ColumnWriterState &state) override;
	void Prepare(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count,
	             bool vector_can_span_multiple_pages) override;

	void BeginWrite(ColumnWriterState &state) override;
	void Write(ColumnWriterState &state, Vector &vector, idx_t count) override;
	void FinalizeWrite(ColumnWriterState &state) override;

	bool HasTransform() override {
		return true;
	}
	LogicalType TransformedType() override;
	unique_ptr<Expression> TransformExpression(unique_ptr<BoundReferenceExpression> expr) override {
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(unique_ptr_cast<BoundReferenceExpression, Expression>(std::move(expr)));

		return make_uniq<BoundFunctionExpression>(TransformedType(), GetTransformFunction(), std::move(arguments),
		                                          nullptr, false);
	}

	ScalarFunction GetTransformFunction();
};

} // namespace duckdb
