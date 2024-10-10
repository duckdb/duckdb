
#include "geo_parquet.hpp"

#include "column_reader.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "expression_column_reader.hpp"
#include "parquet_reader.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

const char *WKBGeometryTypes::ToString(WKBGeometryType type) {
	switch (type) {
	case WKBGeometryType::POINT:
		return "Point";
	case WKBGeometryType::LINESTRING:
		return "LineString";
	case WKBGeometryType::POLYGON:
		return "Polygon";
	case WKBGeometryType::MULTIPOINT:
		return "MultiPoint";
	case WKBGeometryType::MULTILINESTRING:
		return "MultiLineString";
	case WKBGeometryType::MULTIPOLYGON:
		return "MultiPolygon";
	case WKBGeometryType::GEOMETRYCOLLECTION:
		return "GeometryCollection";
	case WKBGeometryType::POINT_Z:
		return "Point Z";
	case WKBGeometryType::LINESTRING_Z:
		return "LineString Z";
	case WKBGeometryType::POLYGON_Z:
		return "Polygon Z";
	case WKBGeometryType::MULTIPOINT_Z:
		return "MultiPoint Z";
	case WKBGeometryType::MULTILINESTRING_Z:
		return "MultiLineString Z";
	case WKBGeometryType::MULTIPOLYGON_Z:
		return "MultiPolygon Z";
	case WKBGeometryType::GEOMETRYCOLLECTION_Z:
		return "GeometryCollection Z";
	default:
		throw NotImplementedException("Unsupported geometry type");
	}
}

//------------------------------------------------------------------------------
// GeoParquetColumnMetadataWriter
//------------------------------------------------------------------------------
GeoParquetColumnMetadataWriter::GeoParquetColumnMetadataWriter(ClientContext &context) {
	executor = make_uniq<ExpressionExecutor>(context);

	auto &catalog = Catalog::GetSystemCatalog(context);

	// These functions are required to extract the geometry type, ZM flag and bounding box from a WKB blob
	auto &type_func_set =
	    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_geometrytype")
	        .Cast<ScalarFunctionCatalogEntry>();
	auto &flag_func_set = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_zmflag")
	                          .Cast<ScalarFunctionCatalogEntry>();
	auto &bbox_func_set = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_extent")
	                          .Cast<ScalarFunctionCatalogEntry>();

	auto wkb_type = LogicalType(LogicalTypeId::BLOB);
	wkb_type.SetAlias("WKB_BLOB");

	auto type_func = type_func_set.functions.GetFunctionByArguments(context, {wkb_type});
	auto flag_func = flag_func_set.functions.GetFunctionByArguments(context, {wkb_type});
	auto bbox_func = bbox_func_set.functions.GetFunctionByArguments(context, {wkb_type});

	auto type_type = LogicalType::UTINYINT;
	auto flag_type = flag_func.return_type;
	auto bbox_type = bbox_func.return_type;

	vector<unique_ptr<Expression>> type_args;
	type_args.push_back(make_uniq<BoundReferenceExpression>(wkb_type, 0));

	vector<unique_ptr<Expression>> flag_args;
	flag_args.push_back(make_uniq<BoundReferenceExpression>(wkb_type, 0));

	vector<unique_ptr<Expression>> bbox_args;
	bbox_args.push_back(make_uniq<BoundReferenceExpression>(wkb_type, 0));

	type_expr = make_uniq<BoundFunctionExpression>(type_type, type_func, std::move(type_args), nullptr);
	flag_expr = make_uniq<BoundFunctionExpression>(flag_type, flag_func, std::move(flag_args), nullptr);
	bbox_expr = make_uniq<BoundFunctionExpression>(bbox_type, bbox_func, std::move(bbox_args), nullptr);

	// Add the expressions to the executor
	executor->AddExpression(*type_expr);
	executor->AddExpression(*flag_expr);
	executor->AddExpression(*bbox_expr);

	// Initialize the input and result chunks
	// The input chunk should be empty, as we always reference the input vector
	input_chunk.InitializeEmpty({wkb_type});
	result_chunk.Initialize(context, {type_type, flag_type, bbox_type});
}

void GeoParquetColumnMetadataWriter::Update(GeoParquetColumnMetadata &meta, Vector &vector, idx_t count) {
	input_chunk.Reset();
	result_chunk.Reset();

	// Reference the vector
	input_chunk.data[0].Reference(vector);
	input_chunk.SetCardinality(count);

	// Execute the expression
	executor->Execute(input_chunk, result_chunk);

	// The first column is the geometry type
	// The second column is the zm flag
	// The third column is the bounding box

	UnifiedVectorFormat type_format;
	UnifiedVectorFormat flag_format;
	UnifiedVectorFormat bbox_format;

	result_chunk.data[0].ToUnifiedFormat(count, type_format);
	result_chunk.data[1].ToUnifiedFormat(count, flag_format);
	result_chunk.data[2].ToUnifiedFormat(count, bbox_format);

	const auto &bbox_components = StructVector::GetEntries(result_chunk.data[2]);
	D_ASSERT(bbox_components.size() == 4);

	UnifiedVectorFormat xmin_format;
	UnifiedVectorFormat ymin_format;
	UnifiedVectorFormat xmax_format;
	UnifiedVectorFormat ymax_format;

	bbox_components[0]->ToUnifiedFormat(count, xmin_format);
	bbox_components[1]->ToUnifiedFormat(count, ymin_format);
	bbox_components[2]->ToUnifiedFormat(count, xmax_format);
	bbox_components[3]->ToUnifiedFormat(count, ymax_format);

	for (idx_t in_idx = 0; in_idx < count; in_idx++) {
		const auto type_idx = type_format.sel->get_index(in_idx);
		const auto flag_idx = flag_format.sel->get_index(in_idx);
		const auto bbox_idx = bbox_format.sel->get_index(in_idx);

		const auto type_valid = type_format.validity.RowIsValid(type_idx);
		const auto flag_valid = flag_format.validity.RowIsValid(flag_idx);
		const auto bbox_valid = bbox_format.validity.RowIsValid(bbox_idx);

		if (!type_valid || !flag_valid || !bbox_valid) {
			continue;
		}

		// Update the geometry type
		const auto flag = UnifiedVectorFormat::GetData<uint8_t>(flag_format)[flag_idx];
		const auto type = UnifiedVectorFormat::GetData<uint8_t>(type_format)[type_idx];
		if (flag == 1 || flag == 3) {
			// M or ZM
			throw InvalidInputException("Geoparquet does not support geometries with M coordinates");
		}
		const auto has_z = flag == 2;
		auto wkb_type = static_cast<WKBGeometryType>((type + 1) + (has_z ? 1000 : 0));
		meta.geometry_types.insert(wkb_type);

		// Update the bounding box
		const auto min_x = UnifiedVectorFormat::GetData<double>(xmin_format)[bbox_idx];
		const auto min_y = UnifiedVectorFormat::GetData<double>(ymin_format)[bbox_idx];
		const auto max_x = UnifiedVectorFormat::GetData<double>(xmax_format)[bbox_idx];
		const auto max_y = UnifiedVectorFormat::GetData<double>(ymax_format)[bbox_idx];
		meta.bbox.Combine(min_x, max_x, min_y, max_y);
	}
}

//------------------------------------------------------------------------------
// GeoParquetFileMetadata
//------------------------------------------------------------------------------

unique_ptr<GeoParquetFileMetadata>
GeoParquetFileMetadata::TryRead(const duckdb_parquet::format::FileMetaData &file_meta_data,
                                const ClientContext &context) {

	// Conversion not enabled, or spatial is not loaded!
	if (!IsGeoParquetConversionEnabled(context)) {
		return nullptr;
	}

	for (auto &kv : file_meta_data.key_value_metadata) {
		if (kv.key == "geo") {
			const auto geo_metadata = yyjson_read(kv.value.c_str(), kv.value.size(), 0);
			if (!geo_metadata) {
				// Could not parse the JSON
				return nullptr;
			}

			try {
				// Check the root object
				const auto root = yyjson_doc_get_root(geo_metadata);
				if (!yyjson_is_obj(root)) {
					throw InvalidInputException("Geoparquet metadata is not an object");
				}

				auto result = make_uniq<GeoParquetFileMetadata>();

				// Check and parse the version
				const auto version_val = yyjson_obj_get(root, "version");
				if (!yyjson_is_str(version_val)) {
					throw InvalidInputException("Geoparquet metadata does not have a version");
				}
				result->version = yyjson_get_str(version_val);
				if (StringUtil::StartsWith(result->version, "2")) {
					// Guard against a breaking future 2.0 version
					throw InvalidInputException("Geoparquet version %s is not supported", result->version);
				}

				// Check and parse the primary geometry column
				const auto primary_geometry_column_val = yyjson_obj_get(root, "primary_column");
				if (!yyjson_is_str(primary_geometry_column_val)) {
					throw InvalidInputException("Geoparquet metadata does not have a primary column");
				}
				result->primary_geometry_column = yyjson_get_str(primary_geometry_column_val);

				// Check and parse the geometry columns
				const auto columns_val = yyjson_obj_get(root, "columns");
				if (!yyjson_is_obj(columns_val)) {
					throw InvalidInputException("Geoparquet metadata does not have a columns object");
				}

				// Iterate over all geometry columns
				yyjson_obj_iter iter = yyjson_obj_iter_with(columns_val);
				yyjson_val *column_key;

				while ((column_key = yyjson_obj_iter_next(&iter))) {
					const auto column_val = yyjson_obj_iter_get_val(column_key);
					const auto column_name = yyjson_get_str(column_key);

					auto &column = result->geometry_columns[column_name];

					if (!yyjson_is_obj(column_val)) {
						throw InvalidInputException("Geoparquet column '%s' is not an object", column_name);
					}

					// Parse the encoding
					const auto encoding_val = yyjson_obj_get(column_val, "encoding");
					if (!yyjson_is_str(encoding_val)) {
						throw InvalidInputException("Geoparquet column '%s' does not have an encoding", column_name);
					}
					const auto encoding_str = yyjson_get_str(encoding_val);
					if (strcmp(encoding_str, "WKB") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::WKB;
					} else {
						throw InvalidInputException("Geoparquet column '%s' has an unsupported encoding", column_name);
					}

					// Parse the geometry types
					const auto geometry_types_val = yyjson_obj_get(column_val, "geometry_types");
					if (!yyjson_is_arr(geometry_types_val)) {
						throw InvalidInputException("Geoparquet column '%s' does not have geometry types", column_name);
					}
					// We dont care about the geometry types for now.

					// TODO: Parse the bounding box, other metadata that might be useful.
					// (Only encoding and geometry types are required to be present)
				}

				// Return the result
				// Make sure to free the JSON document
				yyjson_doc_free(geo_metadata);
				return result;

			} catch (...) {
				// Make sure to free the JSON document in case of an exception
				yyjson_doc_free(geo_metadata);
				throw;
			}
		}
	}
	return nullptr;
}

void GeoParquetFileMetadata::FlushColumnMeta(const string &column_name, const GeoParquetColumnMetadata &meta) {
	// Lock the metadata
	lock_guard<mutex> glock(write_lock);

	auto &column = geometry_columns[column_name];

	// Combine the metadata
	column.geometry_types.insert(meta.geometry_types.begin(), meta.geometry_types.end());
	column.bbox.Combine(meta.bbox);
}

void GeoParquetFileMetadata::Write(duckdb_parquet::format::FileMetaData &file_meta_data) const {

	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	// Add the version
	yyjson_mut_obj_add_strncpy(doc, root, "version", version.c_str(), version.size());

	// Add the primary column
	yyjson_mut_obj_add_strncpy(doc, root, "primary_column", primary_geometry_column.c_str(),
	                           primary_geometry_column.size());

	// Add the columns
	const auto json_columns = yyjson_mut_obj_add_obj(doc, root, "columns");

	for (auto &column : geometry_columns) {
		const auto column_json = yyjson_mut_obj_add_obj(doc, json_columns, column.first.c_str());
		yyjson_mut_obj_add_str(doc, column_json, "encoding", "WKB");
		const auto geometry_types = yyjson_mut_obj_add_arr(doc, column_json, "geometry_types");
		for (auto &geometry_type : column.second.geometry_types) {
			const auto type_name = WKBGeometryTypes::ToString(geometry_type);
			yyjson_mut_arr_add_str(doc, geometry_types, type_name);
		}
		const auto bbox = yyjson_mut_obj_add_arr(doc, column_json, "bbox");
		yyjson_mut_arr_add_real(doc, bbox, column.second.bbox.min_x);
		yyjson_mut_arr_add_real(doc, bbox, column.second.bbox.min_y);
		yyjson_mut_arr_add_real(doc, bbox, column.second.bbox.max_x);
		yyjson_mut_arr_add_real(doc, bbox, column.second.bbox.max_y);

		// If the CRS is present, add it
		if (!column.second.projjson.empty()) {
			const auto crs_doc = yyjson_read(column.second.projjson.c_str(), column.second.projjson.size(), 0);
			if (!crs_doc) {
				yyjson_mut_doc_free(doc);
				throw InvalidInputException("Failed to parse CRS JSON");
			}
			const auto crs_root = yyjson_doc_get_root(crs_doc);
			const auto crs_val = yyjson_val_mut_copy(doc, crs_root);
			const auto crs_key = yyjson_mut_strcpy(doc, "projjson");
			yyjson_mut_obj_add(column_json, crs_key, crs_val);
			yyjson_doc_free(crs_doc);
		}
	}

	yyjson_write_err err;
	size_t len;
	char *json = yyjson_mut_write_opts(doc, 0, nullptr, &len, &err);
	if (!json) {
		yyjson_mut_doc_free(doc);
		throw SerializationException("Failed to write JSON string: %s", err.msg);
	}

	// Create a string from the JSON
	duckdb_parquet::format::KeyValue kv;
	kv.__set_key("geo");
	kv.__set_value(string(json, len));

	// Free the JSON and the document
	free(json);
	yyjson_mut_doc_free(doc);

	file_meta_data.key_value_metadata.push_back(kv);
	file_meta_data.__isset.key_value_metadata = true;
}

bool GeoParquetFileMetadata::IsGeometryColumn(const string &column_name) const {
	return geometry_columns.find(column_name) != geometry_columns.end();
}

void GeoParquetFileMetadata::RegisterGeometryColumn(const string &column_name) {
	lock_guard<mutex> glock(write_lock);
	if (primary_geometry_column.empty()) {
		primary_geometry_column = column_name;
	}
	geometry_columns[column_name] = GeoParquetColumnMetadata();
}

bool GeoParquetFileMetadata::IsGeoParquetConversionEnabled(const ClientContext &context) {
	Value geoparquet_enabled;
	if (!context.TryGetCurrentSetting("enable_geoparquet_conversion", geoparquet_enabled)) {
		return false;
	}
	if (!geoparquet_enabled.GetValue<bool>()) {
		// Disabled by setting
		return false;
	}
	if (!context.db->ExtensionIsLoaded("spatial")) {
		// Spatial extension is not loaded, we cant convert anyway
		return false;
	}
	return true;
}

unique_ptr<ColumnReader> GeoParquetFileMetadata::CreateColumnReader(ParquetReader &reader,
                                                                    const LogicalType &logical_type,
                                                                    const SchemaElement &s_ele, idx_t schema_idx_p,
                                                                    idx_t max_define_p, idx_t max_repeat_p,
                                                                    ClientContext &context) {

	D_ASSERT(IsGeometryColumn(s_ele.name));

	const auto &column = geometry_columns[s_ele.name];

	// Get the catalog
	auto &catalog = Catalog::GetSystemCatalog(context);

	// WKB encoding
	if (logical_type.id() == LogicalTypeId::BLOB && column.geometry_encoding == GeoParquetColumnEncoding::WKB) {
		// Look for a conversion function in the catalog
		auto &conversion_func_set =
		    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_geomfromwkb")
		        .Cast<ScalarFunctionCatalogEntry>();
		auto conversion_func = conversion_func_set.functions.GetFunctionByArguments(context, {LogicalType::BLOB});

		// Create a bound function call expression
		auto args = vector<unique_ptr<Expression>>();
		args.push_back(std::move(make_uniq<BoundReferenceExpression>(LogicalType::BLOB, 0)));
		auto expr =
		    make_uniq<BoundFunctionExpression>(conversion_func.return_type, conversion_func, std::move(args), nullptr);

		// Create a child reader
		auto child_reader =
		    ColumnReader::CreateReader(reader, logical_type, s_ele, schema_idx_p, max_define_p, max_repeat_p);

		// Create an expression reader that applies the conversion function to the child reader
		return make_uniq<ExpressionColumnReader>(context, std::move(child_reader), std::move(expr));
	}

	// Otherwise, unrecognized encoding
	throw NotImplementedException("Unsupported geometry encoding");
}

} // namespace duckdb
