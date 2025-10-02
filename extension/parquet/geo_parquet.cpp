
#include "geo_parquet.hpp"

#include "column_reader.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "reader/expression_column_reader.hpp"
#include "parquet_reader.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

//------------------------------------------------------------------------------
// WKB stats
//------------------------------------------------------------------------------
namespace {

class BinaryReader {
public:
	const char *beg;
	const char *end;
	const char *ptr;

	BinaryReader(const char *beg, uint32_t len) : beg(beg), end(beg + len), ptr(beg) {
	}

	template <class T>
	T Read() {
		if (ptr + sizeof(T) > end) {
			throw InvalidInputException("Unexpected end of WKB data");
		}
		T val;
		memcpy(&val, ptr, sizeof(T));
		ptr += sizeof(T);
		return val;
	}

	void Skip(idx_t len) {
		if (ptr + len > end) {
			throw InvalidInputException("Unexpected end of WKB data");
		}
		ptr += len;
	}

	const char *Reserve(idx_t len) {
		if (ptr + len > end) {
			throw InvalidInputException("Unexpected end of WKB data");
		}
		auto ret = ptr;
		ptr += len;
		return ret;
	}

	bool IsAtEnd() const {
		return ptr >= end;
	}
};

} // namespace

static void UpdateBoundsFromVertexArray(GeometryExtent &bbox, uint32_t flag, const char *vert_array,
                                        uint32_t vert_count) {
	switch (flag) {
	case 0: { // XY
		constexpr auto vert_width = sizeof(double) * 2;
		for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
			double vert[2];
			memcpy(vert, vert_array + vert_idx * vert_width, vert_width);
			bbox.ExtendX(vert[0]);
			bbox.ExtendY(vert[1]);
		}
	} break;
	case 1: { // XYZ
		constexpr auto vert_width = sizeof(double) * 3;
		for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
			double vert[3];
			memcpy(vert, vert_array + vert_idx * vert_width, vert_width);
			bbox.ExtendX(vert[0]);
			bbox.ExtendY(vert[1]);
			bbox.ExtendZ(vert[2]);
		}
	} break;
	case 2: { // XYM
		constexpr auto vert_width = sizeof(double) * 3;
		for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
			double vert[3];
			memcpy(vert, vert_array + vert_idx * vert_width, vert_width);
			bbox.ExtendX(vert[0]);
			bbox.ExtendY(vert[1]);
			bbox.ExtendM(vert[2]);
		}
	} break;
	case 3: { // XYZM
		constexpr auto vert_width = sizeof(double) * 4;
		for (uint32_t vert_idx = 0; vert_idx < vert_count; vert_idx++) {
			double vert[4];
			memcpy(vert, vert_array + vert_idx * vert_width, vert_width);
			bbox.ExtendX(vert[0]);
			bbox.ExtendY(vert[1]);
			bbox.ExtendZ(vert[2]);
			bbox.ExtendM(vert[3]);
		}
	} break;
	default:
		break;
	}
}

void GeometryStats::Update(const string_t &wkb) {
	BinaryReader reader(wkb.GetData(), wkb.GetSize());

	bool first_geom = true;
	while (!reader.IsAtEnd()) {
		reader.Read<uint8_t>(); // byte order
		auto type = reader.Read<uint32_t>();
		auto kind = type % 1000;
		auto flag = type / 1000;
		const auto hasz = (flag & 0x01) != 0;
		const auto hasm = (flag & 0x02) != 0;

		if (first_geom) {
			// Only add the top-level geometry type
			types.Add(type);
			first_geom = false;
		}

		const auto vert_width = sizeof(double) * (2 + (hasz ? 1 : 0) + (hasm ? 1 : 0));

		switch (kind) {
		case 1: { // POINT

			// Point are special in that they are considered "empty" if they are all-nan
			const auto vert_array = reader.Reserve(vert_width);
			const auto dims_count = 2 + (hasz ? 1 : 0) + (hasm ? 1 : 0);
			double vert_point[4] = {0, 0, 0, 0};

			memcpy(vert_point, vert_array, vert_width);

			for (auto dim_idx = 0; dim_idx < dims_count; dim_idx++) {
				if (!std::isnan(vert_point[dim_idx])) {
					bbox.ExtendX(vert_point[0]);
					bbox.ExtendY(vert_point[1]);
					if (hasz && hasm) {
						bbox.ExtendZ(vert_point[2]);
						bbox.ExtendM(vert_point[3]);
					} else if (hasz) {
						bbox.ExtendZ(vert_point[2]);
					} else if (hasm) {
						bbox.ExtendM(vert_point[2]);
					}
					break;
				}
			}
		} break;
		case 2: { // LINESTRING
			const auto vert_count = reader.Read<uint32_t>();
			const auto vert_array = reader.Reserve(vert_count * vert_width);
			UpdateBoundsFromVertexArray(bbox, flag, vert_array, vert_count);
		} break;
		case 3: { // POLYGON
			const auto ring_count = reader.Read<uint32_t>();
			for (uint32_t ring_idx = 0; ring_idx < ring_count; ring_idx++) {
				const auto vert_count = reader.Read<uint32_t>();
				const auto vert_array = reader.Reserve(vert_count * vert_width);
				UpdateBoundsFromVertexArray(bbox, flag, vert_array, vert_count);
			}
		} break;
		case 4:   // MULTIPOINT
		case 5:   // MULTILINESTRING
		case 6:   // MULTIPOLYGON
		case 7: { // GEOMETRYCOLLECTION
			reader.Skip(sizeof(uint32_t));
		} break;
		}
	}
}

//------------------------------------------------------------------------------
// GeoParquetFileMetadata
//------------------------------------------------------------------------------

unique_ptr<GeoParquetFileMetadata> GeoParquetFileMetadata::TryRead(const duckdb_parquet::FileMetaData &file_meta_data,
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
					} else if (strcmp(encoding_str, "point") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::POINT;
					} else if (strcmp(encoding_str, "linestring") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::LINESTRING;
					} else if (strcmp(encoding_str, "polygon") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::POLYGON;
					} else if (strcmp(encoding_str, "multipoint") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::MULTIPOINT;
					} else if (strcmp(encoding_str, "multilinestring") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::MULTILINESTRING;
					} else if (strcmp(encoding_str, "multipolygon") == 0) {
						column.geometry_encoding = GeoParquetColumnEncoding::MULTIPOLYGON;
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

void GeoParquetFileMetadata::AddGeoParquetStats(const string &column_name, const LogicalType &type,
                                                const GeometryStats &stats) {

	// Lock the metadata
	lock_guard<mutex> glock(write_lock);

	auto it = geometry_columns.find(column_name);
	if (it == geometry_columns.end()) {
		auto &column = geometry_columns[column_name];

		column.stats.types.Combine(stats.types);
		column.stats.bbox.Combine(stats.bbox);
		column.insertion_index = geometry_columns.size() - 1;
	} else {
		it->second.stats.types.Combine(stats.types);
		it->second.stats.bbox.Combine(stats.bbox);
	}
}

void GeoParquetFileMetadata::Write(duckdb_parquet::FileMetaData &file_meta_data) {

	// GeoParquet does not support M or ZM coordinates. So remove any columns that have them.
	unordered_set<string> invalid_columns;
	for (auto &column : geometry_columns) {
		if (column.second.stats.bbox.HasM()) {
			invalid_columns.insert(column.first);
		}
	}
	for (auto &col_name : invalid_columns) {
		geometry_columns.erase(col_name);
	}
	// No columns remaining, nothing to write
	if (geometry_columns.empty()) {
		return;
	}

	// Find the primary geometry column
	const auto &random_first_column = *geometry_columns.begin();
	auto primary_geometry_column = random_first_column.first;
	auto primary_insertion_index = random_first_column.second.insertion_index;

	for (auto &column : geometry_columns) {
		if (column.second.insertion_index < primary_insertion_index) {
			primary_insertion_index = column.second.insertion_index;
			primary_geometry_column = column.first;
		}
	}

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
		for (auto &type_name : column.second.stats.types.ToString(false)) {
			yyjson_mut_arr_add_strcpy(doc, geometry_types, type_name.c_str());
		}

		const auto &bbox = column.second.stats.bbox;

		if (bbox.IsSet()) {

			const auto bbox_arr = yyjson_mut_obj_add_arr(doc, column_json, "bbox");

			if (!column.second.stats.bbox.HasZ()) {
				yyjson_mut_arr_add_real(doc, bbox_arr, bbox.xmin);
				yyjson_mut_arr_add_real(doc, bbox_arr, bbox.ymin);
				yyjson_mut_arr_add_real(doc, bbox_arr, bbox.xmax);
				yyjson_mut_arr_add_real(doc, bbox_arr, bbox.ymax);
			} else {
				yyjson_mut_arr_add_real(doc, bbox_arr, bbox.xmin);
				yyjson_mut_arr_add_real(doc, bbox_arr, bbox.ymin);
				yyjson_mut_arr_add_real(doc, bbox_arr, bbox.zmin);
				yyjson_mut_arr_add_real(doc, bbox_arr, bbox.xmax);
				yyjson_mut_arr_add_real(doc, bbox_arr, bbox.ymax);
				yyjson_mut_arr_add_real(doc, bbox_arr, bbox.zmax);
			}
		}

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
	duckdb_parquet::KeyValue kv;
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

LogicalType GeoParquetFileMetadata::GeometryType() {
	auto blob_type = LogicalType(LogicalTypeId::BLOB);
	blob_type.SetAlias("GEOMETRY");
	return blob_type;
}

const unordered_map<string, GeoParquetColumnMetadata> &GeoParquetFileMetadata::GetColumnMeta() const {
	return geometry_columns;
}

unique_ptr<ColumnReader> GeoParquetFileMetadata::CreateColumnReader(ParquetReader &reader,
                                                                    const ParquetColumnSchema &schema,
                                                                    ClientContext &context) {

	// Get the catalog
	auto &catalog = Catalog::GetSystemCatalog(context);

	// WKB encoding
	if (schema.children[0].type.id() == LogicalTypeId::BLOB) {
		// Look for a conversion function in the catalog
		auto &conversion_func_set =
		    catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, "st_geomfromwkb");
		auto conversion_func = conversion_func_set.functions.GetFunctionByArguments(context, {LogicalType::BLOB});

		// Create a bound function call expression
		auto args = vector<unique_ptr<Expression>>();
		args.push_back(std::move(make_uniq<BoundReferenceExpression>(LogicalType::BLOB, 0)));
		auto expr =
		    make_uniq<BoundFunctionExpression>(conversion_func.return_type, conversion_func, std::move(args), nullptr);

		// Create a child reader
		auto child_reader = ColumnReader::CreateReader(reader, schema.children[0]);

		// Create an expression reader that applies the conversion function to the child reader
		return make_uniq<ExpressionColumnReader>(context, std::move(child_reader), std::move(expr), schema);
	}

	// Otherwise, unrecognized encoding
	throw NotImplementedException("Unsupported geometry encoding");
}

} // namespace duckdb
