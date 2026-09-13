#include <string>
#include <utility>
#include <vector>

#include "writer/variant_column_writer.hpp"
#include "column_writer.hpp"
#include "parquet_column_schema.hpp"
#include "parquet_types.h"
#include "duckdb/common/types/variant/parquet_variant_iterator.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

idx_t VariantColumnWriter::FinalizeSchema(vector<duckdb_parquet::SchemaElement> &schemas) {
	idx_t schema_idx = schemas.size();

	auto &schema = Schema();
	schema.SetSchemaIndex(schema_idx);

	auto &repetition_type = schema.repetition_type;
	auto &name = schema.name;
	auto &field_id = schema.field_id;

	// variant group
	duckdb_parquet::SchemaElement top_element;
	top_element.repetition_type = repetition_type;
	top_element.num_children = NumericCast<int32_t>(child_writers.size());
	top_element.logicalType.__isset.VARIANT = true;
	top_element.logicalType.VARIANT.__isset.specification_version = true;
	top_element.logicalType.VARIANT.specification_version = 1;
	top_element.__isset.logicalType = true;
	top_element.__isset.num_children = true;
	top_element.__isset.repetition_type = true;
	top_element.name = name;
	if (field_id.IsValid()) {
		top_element.__isset.field_id = true;
		top_element.field_id = NumericCast<int32_t>(field_id.GetIndex());
	}
	schemas.push_back(std::move(top_element));

	idx_t unique_columns = 0;
	for (auto &child_writer : child_writers) {
		unique_columns += child_writer->FinalizeSchema(schemas);
	}
	return unique_columns;
}

LogicalType VariantColumnWriter::TransformTypedValueRecursive(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		//! Wrap all fields of the struct in a struct with 'value' and 'typed_value' fields
		auto &child_types = StructType::GetChildTypes(type);
		child_list_t<LogicalType> replaced_types;
		for (auto &entry : child_types) {
			child_list_t<LogicalType> child_children;
			child_children.emplace_back("value", LogicalType::BLOB);
			if (entry.second.id() != LogicalTypeId::VARIANT) {
				child_children.emplace_back("typed_value", TransformTypedValueRecursive(entry.second));
			}
			replaced_types.emplace_back(entry.first, LogicalType::STRUCT(child_children));
		}
		return LogicalType::STRUCT(replaced_types);
	}
	case LogicalTypeId::LIST: {
		auto &child_type = ListType::GetChildType(type);
		child_list_t<LogicalType> replaced_types;
		replaced_types.emplace_back("value", LogicalType::BLOB);
		if (child_type.id() != LogicalTypeId::VARIANT) {
			replaced_types.emplace_back("typed_value", TransformTypedValueRecursive(child_type));
		}
		return LogicalType::LIST(LogicalType::STRUCT(replaced_types));
	}
	case LogicalTypeId::UNION:
	case LogicalTypeId::MAP:
	case LogicalTypeId::VARIANT:
	case LogicalTypeId::ARRAY:
		throw BinderException("'%s' can't appear inside a 'typed_value' shredded type!", type.ToString());
	default:
		return type;
	}
}

static LogicalType GetParquetVariantType(optional_ptr<LogicalType> shredding = nullptr) {
	child_list_t<LogicalType> children;
	children.emplace_back("metadata", LogicalType::BLOB);
	children.emplace_back("value", LogicalType::BLOB);
	if (shredding && shredding->id() != LogicalTypeId::VARIANT) {
		children.emplace_back("typed_value", VariantColumnWriter::TransformTypedValueRecursive(*shredding));
	}
	return LogicalType::STRUCT(std::move(children)).WithAlias("PARQUET_VARIANT");
}

static unique_ptr<FunctionData> BindTransform(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	if (arguments.empty()) {
		return nullptr;
	}
	auto type = ExpressionBinder::GetExpressionReturnType(*arguments[0]);

	if (arguments.size() == 2) {
		auto &shredding = *arguments[1];
		auto expr_return_type = ExpressionBinder::GetExpressionReturnType(shredding);
		expr_return_type = LogicalType::NormalizeType(expr_return_type);
		if (expr_return_type.id() != LogicalTypeId::VARCHAR) {
			throw BinderException("Optional second argument 'shredding' has to be of type VARCHAR, i.e: "
			                      "'STRUCT(my_field BOOLEAN)', found type: '%s' instead",
			                      expr_return_type);
		}
		auto type_str = input.GetNonNullConstant(1);
		auto shredded_type = TransformStringToLogicalType(type_str.GetValue<string>(), context);
		bound_function.SetReturnType(GetParquetVariantType(shredded_type));
	} else {
		bound_function.SetReturnType(GetParquetVariantType());
	}

	return nullptr;
}

//! The conversion itself lives in core (ParquetVariantConversion::ToParquetVariant), so the
//! arrow.parquet.variant extension type can call it directly without going through the binder.
static void ToParquetVariantFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	ParquetVariantConversion::ToParquetVariant(input.data[0], input.size(), result);
}

ScalarFunction VariantColumnWriter::GetTransformFunction() {
	ScalarFunction transform("variant_to_parquet_variant", {{"variant", LogicalType::VARIANT()}}, LogicalType::ANY,
	                         ToParquetVariantFunction, BindTransform);
	transform.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	// throws for values that are out of range for the parquet variant encoding
	transform.SetFallible();
	return transform;
}

} // namespace duckdb
