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
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

using variant_type_map = array<idx_t, static_cast<uint8_t>(VariantLogicalType::ENUM_SIZE)>;

struct ObjectAnalyzeData;
struct ArrayAnalyzeData;

struct VariantAnalyzeData {
public:
	VariantAnalyzeData() {
	}

public:
	//! Map for every value what type it is
	variant_type_map type_map = {};
	//! Map for every decimal value what physical type it has
	array<idx_t, 3> decimal_type_map = {};
	unique_ptr<ObjectAnalyzeData> object_data = nullptr;
	unique_ptr<ArrayAnalyzeData> array_data = nullptr;
};

struct ObjectAnalyzeData {
public:
	ObjectAnalyzeData() {
	}

public:
	case_insensitive_map_t<VariantAnalyzeData> fields;
};

struct ArrayAnalyzeData {
public:
	ArrayAnalyzeData() {
	}

public:
	VariantAnalyzeData child;
};

struct VariantAnalyzeSchemaState : public ParquetAnalyzeSchemaState {
public:
	VariantAnalyzeSchemaState() {
	}
	~VariantAnalyzeSchemaState() override {
	}

public:
	VariantAnalyzeData analyze_data;
};

class VariantColumnWriter : public StructColumnWriter {
public:
	VariantColumnWriter(ParquetWriter &writer, ParquetColumnSchema &&column_schema, vector<string> schema_path_p,
	                    vector<unique_ptr<ColumnWriter>> child_writers_p)
	    : StructColumnWriter(writer, std::move(column_schema), std::move(schema_path_p), std::move(child_writers_p)) {
	}
	~VariantColumnWriter() override = default;

public:
	void FinalizeSchema(vector<duckdb_parquet::SchemaElement> &schemas) override;
	unique_ptr<ParquetAnalyzeSchemaState> AnalyzeSchemaInit() override;
	void AnalyzeSchema(ParquetAnalyzeSchemaState &state, Vector &input, idx_t count) override;
	void AnalyzeSchemaFinalize(const ParquetAnalyzeSchemaState &state) override;

	bool HasTransform() override {
		return true;
	}
	LogicalType TransformedType() override {
		child_list_t<LogicalType> children;
		for (auto &writer : child_writers) {
			auto &child_name = writer->Schema().name;
			auto &child_type = writer->Schema().type;
			children.emplace_back(child_name, child_type);
		}
		return LogicalType::STRUCT(std::move(children));
	}
	unique_ptr<Expression> TransformExpression(unique_ptr<BoundReferenceExpression> expr) override {
		vector<unique_ptr<Expression>> arguments;
		arguments.push_back(unique_ptr_cast<BoundReferenceExpression, Expression>(std::move(expr)));

		return make_uniq<BoundFunctionExpression>(TransformedType(), GetTransformFunction(), std::move(arguments),
		                                          nullptr, false);
	}

public:
	static ScalarFunction GetTransformFunction();
	static LogicalType TransformTypedValueRecursive(const LogicalType &type);
};

} // namespace duckdb
