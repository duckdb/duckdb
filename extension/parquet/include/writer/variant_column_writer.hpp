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
	VariantColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema, vector<string> schema_path_p,
	                    vector<unique_ptr<ColumnWriter>> child_writers_p, bool can_have_nulls)
	    : StructColumnWriter(writer, column_schema, std::move(schema_path_p), std::move(child_writers_p),
	                         can_have_nulls) {
	}
	~VariantColumnWriter() override = default;

public:
	static ScalarFunction GetTransformFunction();
	static LogicalType TransformTypedValueRecursive(const LogicalType &type);
};

} // namespace duckdb
