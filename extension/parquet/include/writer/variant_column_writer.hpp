//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/variant_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "struct_column_writer.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

class VariantColumnWriter : public StructColumnWriter {
public:
	VariantColumnWriter(ParquetWriter &writer, ParquetColumnSchema &column_schema, vector<string> schema_path_p,
	                    vector<unique_ptr<ColumnWriter>> child_writers_p)
	    : StructColumnWriter(writer, column_schema, std::move(schema_path_p), std::move(child_writers_p)) {
	}
	~VariantColumnWriter() override = default;

public:
	void FinalizeSchema(vector<duckdb_parquet::SchemaElement> &schemas) override;

public:
	static ScalarFunction GetTransformFunction();
	static LogicalType TransformTypedValueRecursive(const LogicalType &type);
};

} // namespace duckdb
