
#include "geo_parquet.hpp"
#include "parquet_reader.hpp"
#include "column_reader.hpp"
#include "expression_column_reader.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"

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
// WKB Scanner
//------------------------------------------------------------------------------
class WKBScanner {
	GeoParquetColumnMetadata &data;
	const_data_ptr_t ptr;
	const_data_ptr_t end;

public:
	explicit WKBScanner(GeoParquetColumnMetadata &data) : data(data), ptr(nullptr), end(nullptr) {
	}

	void Scan(const string_t &blob) {
		ptr = const_data_ptr_cast(blob.GetDataUnsafe());
		end = ptr + blob.GetSize();
		ScanWKB(true);
	}

private:
	void ScanWKB(bool first);
	void ScanWKB(WKBGeometryType type, bool le, bool has_z);
	uint32_t ReadInt(bool le);
	double ReadDouble(bool le);
	bool ReadBool();

	template <class T>
	T ReadLE();

	template <class T>
	T ReadBE();
};

void WKBScanner::ScanWKB(const bool first) {
	const auto le = ReadBool();
	const auto type_id = ReadInt(le);
	const bool has_z = type_id > 1000;
	const bool has_m = type_id > 2000;
	const auto type = static_cast<WKBGeometryType>(type_id % 1000);
	if (has_m) {
		// We dont support M!
		throw InvalidInputException("Geoparquet does not support geometries with M coordinates");
	}

	ScanWKB(type, le, has_z);

	if (first) {
		// Only consider the first geometry type
		data.geometry_types.insert(static_cast<WKBGeometryType>(type_id));
	}
}

void WKBScanner::ScanWKB(const WKBGeometryType type, const bool le, const bool has_z) {
	switch (type) {
	case WKBGeometryType::POINT: {
		// Points are special in that they may be empty (all NaN)
		bool all_nan = true;
		double coords[3];
		for (auto i = 0; i < (2 + has_z); i++) {
			coords[i] = ReadDouble(le);
			if (!std::isnan(coords[i])) {
				all_nan = false;
			}
		}
		if (!all_nan) {
			data.bbox.Combine(coords[0], coords[1]);
		}
	} break;
	case WKBGeometryType::LINESTRING: {
		const auto num_points = ReadInt(le);
		for (uint32_t i = 0; i < num_points; i++) {
			const auto x = ReadDouble(le);
			const auto y = ReadDouble(le);
			if (has_z) {
				// We dont care about Z for now
				ptr += sizeof(double);
			}
			data.bbox.Combine(x, y);
		}
	} break;
	case WKBGeometryType::POLYGON: {
		const auto num_rings = ReadInt(le);
		for (uint32_t i = 0; i < num_rings; i++) {
			const auto num_points = ReadInt(le);
			for (uint32_t j = 0; j < num_points; j++) {
				const auto x = ReadDouble(le);
				const auto y = ReadDouble(le);
				if (has_z) {
					// We dont care about Z for now
					ptr += sizeof(double);
				}
				data.bbox.Combine(x, y);
			}
		}
	} break;
	case WKBGeometryType::MULTIPOINT:
	case WKBGeometryType::MULTILINESTRING:
	case WKBGeometryType::MULTIPOLYGON:
	case WKBGeometryType::GEOMETRYCOLLECTION: {
		const auto num_geometries = ReadInt(le);
		for (uint32_t i = 0; i < num_geometries; i++) {
			ScanWKB(false);
		}
	} break;
	default:
		throw SerializationException("Unsupported geometry type");
	}
}

bool WKBScanner::ReadBool() {
	return ReadLE<uint8_t>() != 0;
}

uint32_t WKBScanner::ReadInt(const bool le) {
	return le ? ReadLE<uint32_t>() : ReadBE<uint32_t>();
}

double WKBScanner::ReadDouble(const bool le) {
	return le ? ReadLE<double>() : ReadBE<double>();
}

template <class T>
T WKBScanner::ReadLE() {
	if (ptr + sizeof(T) > end) {
		throw SerializationException("WKB data is too short");
	}
	T res = Load<T>(ptr);
	ptr += sizeof(T);
	return res;
}

template <class T>
T WKBScanner::ReadBE() {
	if (ptr + sizeof(T) > end) {
		throw SerializationException("WKB data is too short");
	}
	data_t in[sizeof(T)];
	data_t out[sizeof(T)];
	memcpy(in, ptr, sizeof(T));
	// Reverse the bytes
	for (idx_t i = 0; i < sizeof(T); i++) {
		out[i] = in[sizeof(T) - i - 1];
	}
	T res;
	memcpy(&res, out, sizeof(T));
	ptr += sizeof(T);
	return res;
}

//------------------------------------------------------------------------------
// GeometryColumnProps
//------------------------------------------------------------------------------
void GeoParquetColumnMetadata::Update(Vector &vector, idx_t count) {
	const auto &data_type = vector.GetType();
	const auto type_id = data_type.id();
	const auto type_name = data_type.GetAlias();

	if (type_id == LogicalTypeId::BLOB && type_name == "WKB_BLOB") {
		UnifiedVectorFormat format;
		vector.ToUnifiedFormat(count, format);
		const auto data = UnifiedVectorFormat::GetData<string_t>(format);

		WKBScanner scanner(*this);
		for (idx_t i = 0; i < count; i++) {
			const auto row_idx = format.sel->get_index(i);
			if (!format.validity.RowIsValid(row_idx)) {
				continue;
			}
			scanner.Scan(data[row_idx]);
		}

	} else {
		throw NotImplementedException("Unsupported geometry encoding %s", type_name);
	}
}

//------------------------------------------------------------------------------
// GeoParquetMetadata
//------------------------------------------------------------------------------

unique_ptr<GeoParquetFileMetadata>
GeoParquetFileMetadata::Read(const duckdb_parquet::format::FileMetaData &file_meta_data) {
	for (auto &kv : file_meta_data.key_value_metadata) {
		if (kv.key == "geo") {
			const auto geo_metadata = yyjson_read(kv.value.c_str(), kv.value.size(), 0);
			if (!geo_metadata) {
				throw InvalidInputException("Failed to parse geoparquet metadata");
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

	// TODO: Keep order of columns
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

bool GeoParquetFileMetadata::IsSpatialExtensionInstalled(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(context);

	// Look for ST_GeomFromWKB in the catalog
	// TODO: Check for more functions
	try {
		auto &conversion_func_set =
		    catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, "st_geomfromwkb")
		        .Cast<ScalarFunctionCatalogEntry>();
		auto _ = conversion_func_set.functions.GetFunctionByArguments(context, {LogicalType::BLOB});
		return true;
	} catch (...) {
		return false;
	}
}

} // namespace duckdb
