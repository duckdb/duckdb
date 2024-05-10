//===----------------------------------------------------------------------===//
//                         DuckDB
//
// geo_parquet.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <algorithm>
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/common/json/json_value.hpp"
#include "parquet_types.h"

namespace duckdb {

struct Bounds {
	double min_x;
	double max_x;
	double min_y;
	double max_y;

	Bounds()
	    : min_x(std::numeric_limits<double>::max()), max_x(std::numeric_limits<double>::lowest()),
	      min_y(std::numeric_limits<double>::max()), max_y(std::numeric_limits<double>::lowest()) {
	}

	Bounds(double min_x, double max_x, double min_y, double max_y)
	    : min_x(min_x), max_x(max_x), min_y(min_y), max_y(max_y) {
	}

	void Combine(const Bounds &other) {
		min_x = std::min(min_x, other.min_x);
		max_x = std::max(max_x, other.max_x);
		min_y = std::min(min_y, other.min_y);
		max_y = std::max(max_y, other.max_y);
	}
};

struct WriteGeoParquetColumnData {
	string name;
	Bounds bounds;
	unordered_set<uint8_t> geometry_types;

	explicit WriteGeoParquetColumnData(string name) : name(std::move(name)) {
	}

	void Combine(const WriteGeoParquetColumnData &other) {
		bounds.Combine(other.bounds);
		geometry_types.insert(other.geometry_types.begin(), other.geometry_types.end());
	}
};

struct WriteGeoParquetLocalState {
	// DataChunk input_chunk;

	DataChunk conversion_output_chunk;
	DataChunk meta_input_chunk;
	DataChunk meta_output_chunk;

	ExpressionExecutor conversion_executor;
	ExpressionExecutor meta_executor;

	unique_ptr<Expression> extent_expr;
	vector<unique_ptr<Expression>> conversion_exprs;
	// unique_ptr<Expression> to_wkb_expr;

	vector<unique_ptr<WriteGeoParquetColumnData>> geo_columns;

	WriteGeoParquetLocalState(ClientContext &context, const vector<LogicalType> &types, const vector<string> &names)
	    : conversion_executor(context), meta_executor(context) {

		// Create expression executor
		auto box_type = LogicalType::STRUCT({{"min_x", LogicalType::DOUBLE},
		                                     {"min_y", LogicalType::DOUBLE},
		                                     {"max_x", LogicalType::DOUBLE},
		                                     {"max_y", LogicalType::DOUBLE}});
		box_type.SetAlias("BOX_2D");

		auto geometry_type = LogicalType(LogicalTypeId::BLOB);
		geometry_type.SetAlias("GEOMETRY");

		auto &catalog = Catalog::GetSystemCatalog(context);

		// Look for a conversion function in the catalog
		auto &to_wkb_func_set =
		    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "ST_AsWKB")
		        .Cast<ScalarFunctionCatalogEntry>();
		auto to_wkb_func = to_wkb_func_set.functions.GetFunctionByArguments(context, {geometry_type});

		// Now setup the metadata for the geometry columns
		vector<LogicalType> converted_types;
		for (idx_t i = 0; i < types.size(); i++) {
			if (types[i].id() == LogicalTypeId::BLOB && types[i].GetAlias() == "GEOMETRY") {
				geo_columns.emplace_back(make_uniq<WriteGeoParquetColumnData>(names[i]));
				converted_types.push_back(LogicalType::BLOB);

				// Create a bound function call expression
				vector<unique_ptr<Expression>> to_wkb_args;
				to_wkb_args.push_back(std::move(make_uniq<BoundReferenceExpression>(geometry_type, i)));
				conversion_exprs.push_back(std::move(make_uniq<BoundFunctionExpression>(
				    LogicalType::BLOB, to_wkb_func, std::move(to_wkb_args), nullptr)));
				conversion_executor.AddExpression(*conversion_exprs.back());
			} else {
				geo_columns.emplace_back();
				converted_types.push_back(types[i]);

				// Just reference the input column
				auto ref_expr = make_uniq<BoundReferenceExpression>(types[i], i);
				conversion_exprs.push_back(std::move(ref_expr));
				conversion_executor.AddExpression(*conversion_exprs.back());
			}
		}

		// Initialize the conversion executor
		conversion_output_chunk.Initialize(context, converted_types);

		// Also setup the expression executor for the metadata
		meta_input_chunk.Initialize(context, {geometry_type});
		meta_output_chunk.Initialize(context, {box_type});

		// Look for a extent function in the catalog
		auto &extent_func_set =
		    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "ST_Extent")
		        .Cast<ScalarFunctionCatalogEntry>();
		auto extent_func = extent_func_set.functions.GetFunctionByArguments(context, {geometry_type});

		// Create a bound function call expression
		vector<unique_ptr<Expression>> extent_args;
		extent_args.push_back(std::move(make_uniq<BoundReferenceExpression>(geometry_type, 0UL)));
		extent_expr =
		    make_uniq<BoundFunctionExpression>(extent_func.return_type, extent_func, std::move(extent_args), nullptr);

		meta_executor.AddExpression(*extent_expr);
	}

	DataChunk &ConvertGeometry(DataChunk &input) {

		for (idx_t i = 0; i < input.ColumnCount(); i++) {
			auto &geo_column = geo_columns[i];
			if (geo_column) {
				meta_input_chunk.data[0].Reference(input.data[i]);
				meta_input_chunk.SetCardinality(input);
				meta_executor.Execute(meta_input_chunk, meta_output_chunk);

				// Update the bounds
				UpdateMeta(meta_output_chunk, *geo_column);
			}
		}

		// Convert the geometry
		conversion_output_chunk.SetCardinality(input);
		conversion_executor.Execute(input, conversion_output_chunk);

		return conversion_output_chunk;
	}

	void UpdateMeta(DataChunk &meta_chunk, WriteGeoParquetColumnData &data) {

		auto count = meta_chunk.size();
		auto &bounds_vec = meta_chunk.data[0];
		auto &bounds_data = StructVector::GetEntries(bounds_vec);
		auto bounds_min_x_data = FlatVector::GetData<double>(*bounds_data[0]);
		auto bounds_min_y_data = FlatVector::GetData<double>(*bounds_data[1]);
		auto bounds_max_x_data = FlatVector::GetData<double>(*bounds_data[2]);
		auto bounds_max_y_data = FlatVector::GetData<double>(*bounds_data[3]);

		UnifiedVectorFormat bounds_format;
		bounds_vec.ToUnifiedFormat(count, bounds_format);
		for (idx_t i = 0; i < count; i++) {
			auto row_id = bounds_format.sel->get_index(i);
			if (!bounds_format.validity.RowIsValid(row_id)) {
				continue;
			}

			auto min_x = bounds_min_x_data[row_id];
			auto min_y = bounds_min_y_data[row_id];
			auto max_x = bounds_max_x_data[row_id];
			auto max_y = bounds_max_y_data[row_id];
			data.bounds.Combine(Bounds {min_x, max_x, min_y, max_y});
		}

		auto &geometry_type_vec = meta_chunk.data[1];
		UnifiedVectorFormat geometry_type_format;
		geometry_type_vec.ToUnifiedFormat(count, geometry_type_format);
		auto geometry_type_data = UnifiedVectorFormat::GetData<uint8_t>(geometry_type_format);

		for (idx_t i = 0; i < count; i++) {
			auto row_id = geometry_type_format.sel->get_index(i);
			if (!geometry_type_format.validity.RowIsValid(row_id)) {
				continue;
			}
			data.geometry_types.insert(geometry_type_data[row_id]);
		}
	}
};

struct WriteGeoParquetGlobalState {
	vector<unique_ptr<WriteGeoParquetColumnData>> geo_columns;

	void Combine(WriteGeoParquetLocalState &local_state) {

		// If this is the first local state, just move the data
		if (geo_columns.empty()) {
			geo_columns = std::move(local_state.geo_columns);
		} else {
			// Otherwise, combine the data
			for (idx_t i = 0; i < geo_columns.size(); i++) {
				auto &local_geo_column = local_state.geo_columns[i];
				if (local_geo_column) {
					geo_columns[i]->Combine(*local_geo_column);
				}
			}
		}
	}

	void FinalizeWrite(duckdb_parquet::format::FileMetaData file_meta_data) {
		JsonValue json;
		json["version"] = 1.0;
		bool has_primary = false;
		auto columns_json = JsonValue(JsonKind::OBJECT);
		for (auto &column : geo_columns) {
			if (!column) {
				continue;
			}
			auto &col = *column;

			if (!has_primary) {
				json["primary_column"] = col.name;
				has_primary = true;
			}

			auto column_json = JsonValue(JsonKind::OBJECT);
			column_json["encoding"] = "WKB";
			column_json["geometry_types"] = JsonValue(JsonKind::ARRAY); // TODO: populate
			column_json["bbox"] = JsonValue(JsonKind::ARRAY);
			column_json["bbox"].AsArray().resize(4); // min_x, min_y, max_x, max_y
			column_json["bbox"][0] = col.bounds.min_x;
			column_json["bbox"][1] = col.bounds.min_y;
			column_json["bbox"][2] = col.bounds.max_x;
			column_json["bbox"][3] = col.bounds.max_y;
			columns_json[col.name] = std::move(column_json);
		}
		json["columns"] = std::move(columns_json);
		duckdb_parquet::format::KeyValue kv;
		kv.__set_key("geo");
		kv.__set_value(json.ToString(false));

		file_meta_data.key_value_metadata.push_back(kv);
		file_meta_data.__isset.key_value_metadata = true;
	}
};

} // namespace duckdb
