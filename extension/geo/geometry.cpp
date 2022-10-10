#include "geometry.hpp"

#include "duckdb/common/types/vector.hpp"
#include "postgis.hpp"

namespace duckdb {

string Geometry::GetString(string_t geometry, DataFormatType ftype) {
	auto data = (const_data_ptr_t)geometry.GetDataUnsafe();
	auto len = geometry.GetSize();
	Postgis postgis;
	string text = "";
	switch (ftype) {
	case DataFormatType::FORMAT_VALUE_TYPE_WKB:
		text = postgis.LWGEOM_asBinary(data, len);
		break;

	case DataFormatType::FORMAT_VALUE_TYPE_WKT:
		text = postgis.LWGEOM_asText(data, len);
		break;

	case DataFormatType::FORMAT_VALUE_TYPE_GEOJSON:
		text = postgis.LWGEOM_asGeoJson(data, len);
		break;

	default:
		break;
	}
	return text;
}

// void Geometry::ToString(string_t geometry, char *output, DataFormatType ftype) {
// 	auto text = Geometry::GetString(geometry, ftype);
// 	auto len = text.size();
// 	memcpy(output, text.c_str(), len);
// }

// string Geometry::ToString(string_t geometry, DataFormatType ftype) {
// 	auto text = Geometry::GetString(geometry, ftype);
// 	auto str_len = text.size();
// 	auto buffer = std::unique_ptr<char[]>(new char[str_len]);
// 	memcpy(buffer.get(), text.c_str(), str_len);
// 	return string(buffer.get(), str_len);
// }

void Geometry::ToGeometry(GSERIALIZED *gser, data_ptr_t output) {
	Postgis postgis;
	data_ptr_t base = (data_ptr_t)postgis.LWGEOM_base(gser);
	auto geometry_len = Geometry::GetGeometrySize(gser);
	memcpy(output, base, geometry_len);
}

string Geometry::ToGeometry(GSERIALIZED *gser) {
	auto geometry_len = Geometry::GetGeometrySize(gser);
	auto buffer = std::unique_ptr<char[]>(new char[geometry_len]);
	Geometry::ToGeometry(gser, (data_ptr_t)buffer.get());
	return string(buffer.get(), geometry_len);
}

// string Geometry::ToGeometry(string_t text) {
// 	auto gser = Geometry::ToGserialized(text);
// 	auto str = Geometry::ToGeometry(gser);
// 	Geometry::DestroyGeometry(gser);
// 	return str;
// }

// GSERIALIZED *Geometry::GetGserialized(string_t geom) {
// 	Postgis postgis;
// 	auto data = (const_data_ptr_t)geom.GetDataUnsafe();
// 	auto size = geom.GetSize();
// 	return postgis.LWGEOM_getGserialized(data, size);
// }

GSERIALIZED *Geometry::ToGserialized(string_t str) {
	Postgis postgis;
	auto ger = postgis.LWGEOM_in(&str.GetString()[0]);
	return ger;
}

idx_t Geometry::GetGeometrySize(GSERIALIZED *gser) {
	Postgis postgis;
	auto gsize = postgis.LWGEOM_size(gser);
	return gsize;
}

void Geometry::DestroyGeometry(GSERIALIZED *gser) {
	Postgis postgis;
	postgis.LWGEOM_free(gser);
}

} // namespace duckdb
