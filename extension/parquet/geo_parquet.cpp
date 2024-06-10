
#include "geo_parquet.hpp"
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson;

//------------------------------------------------------------------------------
// GeometryTypes
//------------------------------------------------------------------------------
const char *GeometryTypes::ToString(const GeometryType type) {
	switch (type) {
	case GeometryType::POINT:
		return "POINT";
	case GeometryType::LINESTRING:
		return "LINESTRING";
	case GeometryType::POLYGON:
		return "POLYGON";
	case GeometryType::MULTIPOINT:
		return "MULTIPOINT";
	case GeometryType::MULTILINESTRING:
		return "MULTILINESTRING";
	case GeometryType::MULTIPOLYGON:
		return "MULTIPOLYGON";
	case GeometryType::GEOMETRYCOLLECTION:
		return "GEOMETRYCOLLECTION";
	default:
		throw NotImplementedException("Unsupported geometry type");
	}
}

//------------------------------------------------------------------------------
// WKB Scanner
//------------------------------------------------------------------------------
class WKBScanner {
	GeometryColumnData &data;
	const_data_ptr_t ptr;
	const_data_ptr_t end;

public:
	explicit WKBScanner(GeometryColumnData &data) : data(data), ptr(nullptr), end(nullptr) {
	}

	void Scan(const string_t &blob) {
		ptr = const_data_ptr_cast(blob.GetDataUnsafe());
		end = ptr + blob.GetSize();
		ScanWKB(true);
	}

private:
	void ScanWKB(bool first);

	template <bool BE, CoordType COORD>
	void ScanWKB(GeometryType type);

	template <bool BE, CoordType COORD>
	void ScanCoord();

	uint32_t ReadInt(bool byte_order);
	double ReadDouble(bool byte_order);
	bool ReadBool();

	template <class T>
	T ReadLE();

	template <class T>
	T ReadBE();
};

template <bool BE, CoordType COORD>
void WKBScanner::ScanCoord() {
	const auto x = ReadDouble(BE);
	const auto y = ReadDouble(BE);
	if (COORD == CoordType::XY) {
		data.bounds.Combine(x, y);
	}
	if (COORD == CoordType::XYM) {
		data.bounds.Combine(x, y);
		// GeoParquest does not support M, so skip it
		ptr += sizeof(double);
	}
	if (COORD == CoordType::XYZ) {
		const auto z = ReadDouble(BE);
		data.bounds.Combine(x, y, z);
	}
	if (COORD == CoordType::XYZM) {
		const auto z = ReadDouble(BE);
		data.bounds.Combine(x, y, z);
		// GeoParquest does not support M, so skip it
		ptr += sizeof(double);
	}
}

void WKBScanner::ScanWKB(const bool first) {
	const auto byte_order = ReadBool();
	const auto type = static_cast<GeometryType>(ReadInt(byte_order));
	if (byte_order) {
		ScanWKB<true, CoordType::XY>(type);
	} else {
		ScanWKB<false, CoordType::XY>(type);
	}
	if (first) {
		// Only consider the first geometry type
		data.types.insert(type);
	}
}

template <bool BE, CoordType COORD>
void WKBScanner::ScanWKB(GeometryType type) {
	switch (type) {
	case GeometryType::POINT: {
		// TODO: Handle NaN for empty points...?
		ScanCoord<BE, COORD>();
	} break;
	case GeometryType::LINESTRING: {
		const auto num_points = ReadInt(BE);
		for (uint32_t i = 0; i < num_points; i++) {
			ScanCoord<BE, COORD>();
		}
	} break;
	case GeometryType::POLYGON: {
		const auto num_rings = ReadInt(BE);
		for (uint32_t i = 0; i < num_rings; i++) {
			const auto num_points = ReadInt(BE);
			for (uint32_t j = 0; j < num_points; j++) {
				ScanCoord<BE, COORD>();
			}
		}
	} break;
	case GeometryType::MULTIPOINT:
	case GeometryType::MULTILINESTRING:
	case GeometryType::MULTIPOLYGON:
	case GeometryType::GEOMETRYCOLLECTION: {
		const auto num_geometries = ReadInt(BE);
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

uint32_t WKBScanner::ReadInt(bool byte_order) {
	return byte_order ? ReadBE<uint32_t>() : ReadLE<uint32_t>();
}

double WKBScanner::ReadDouble(bool byte_order) {
	return byte_order ? ReadBE<double>() : ReadLE<double>();
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
void GeometryColumnData::Update(Vector &vector, idx_t count) {
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
// GeoParquetData
//------------------------------------------------------------------------------
void GeoParquetData::WriteMetadata(duckdb_parquet::format::FileMetaData &file_meta_data) const {
	yyjson_mut_doc *doc = yyjson_mut_doc_new(nullptr);
	yyjson_mut_val *root = yyjson_mut_obj(doc);
	yyjson_mut_doc_set_root(doc, root);

	yyjson_mut_obj_add_str(doc, root, "version", "1.0");
	const auto json_columns = yyjson_mut_obj_add_obj(doc, root, "columns");

	// TODO: Keep order of columns
	for (auto &column : columns) {
		const auto column_json = yyjson_mut_obj_add_obj(doc, json_columns, column.first.c_str());
		yyjson_mut_obj_add_str(doc, column_json, "encoding", "WKB");
		const auto geometry_types = yyjson_mut_obj_add_arr(doc, column_json, "geometry_types");
		for (auto &geometry_type : column.second.types) {
			auto type_name = GeometryTypes::ToString(geometry_type);
			yyjson_mut_arr_add_str(doc, geometry_types, type_name);
		}
		const auto bbox = yyjson_mut_obj_add_arr(doc, column_json, "bbox");
		yyjson_mut_arr_add_real(doc, bbox, column.second.bounds.min_x);
		yyjson_mut_arr_add_real(doc, bbox, column.second.bounds.min_y);
		yyjson_mut_arr_add_real(doc, bbox, column.second.bounds.max_x);
		yyjson_mut_arr_add_real(doc, bbox, column.second.bounds.max_y);
	}

	yyjson_write_err err;
	size_t len;
	constexpr yyjson_write_flag flags = YYJSON_WRITE_ALLOW_INVALID_UNICODE;
	char *json = yyjson_mut_write_opts(doc, flags, nullptr, &len, &err);
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

} // namespace duckdb
