#include "geometry.hpp"

#include "duckdb/common/types/vector.hpp"
#include "postgis.hpp"

namespace duckdb {

string Geometry::GetString(string_t geometry, DataFormatType ftype) {
	auto data = (const_data_ptr_t)geometry.GetDataUnsafe();
	auto len = geometry.GetSize();
	if (len == 0) {
		return "";
	}
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

void Geometry::ToString(string_t geometry, char *output, DataFormatType ftype) {
	auto text = Geometry::GetString(geometry, ftype);
	auto len = text.size();
	memcpy(output, text.c_str(), len);
}

string Geometry::ToString(string_t geometry, DataFormatType ftype) {
	auto text = Geometry::GetString(geometry, ftype);
	auto str_len = text.size();
	auto buffer = std::unique_ptr<char[]>(new char[str_len]);
	memcpy(buffer.get(), text.c_str(), str_len);
	return string(buffer.get(), str_len);
}

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

string Geometry::ToGeometry(string_t text) {
	auto gser = Geometry::ToGserialized(text);
	auto str = Geometry::ToGeometry(gser);
	Geometry::DestroyGeometry(gser);
	return str;
}

GSERIALIZED *Geometry::GetGserialized(string_t geom) {
	Postgis postgis;
	auto data = (const_data_ptr_t)geom.GetDataUnsafe();
	auto size = geom.GetSize();
	return postgis.LWGEOM_getGserialized(data, size);
}

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

data_ptr_t Geometry::GetBase(GSERIALIZED *gser) {
	Postgis postgis;
	data_ptr_t base = (data_ptr_t)postgis.LWGEOM_base(gser);
	return base;
}

GSERIALIZED *Geometry::MakePoint(double x, double y) {
	Postgis postgis;
	return postgis.LWGEOM_makepoint(x, y);
}

GSERIALIZED *Geometry::MakePoint(double x, double y, double z) {
	Postgis postgis;
	return postgis.LWGEOM_makepoint(x, y, z);
}

std::string Geometry::AsText(data_ptr_t base, size_t size) {
	Postgis postgis;
	return postgis.LWGEOM_asText(base, size);
}

double Geometry::Distance(GSERIALIZED *g1, GSERIALIZED *g2) {
	Postgis postgis;
	return postgis.ST_distance(g1, g2);
}

double Geometry::Distance(GSERIALIZED *g1, GSERIALIZED *g2, bool use_spheroid) {
	Postgis postgis;
	return postgis.geography_distance(g1, g2, use_spheroid);
}

GSERIALIZED *Geometry::FromText(char *text) {
	Postgis postgis;
	return postgis.LWGEOM_from_text(text);
}

GSERIALIZED *Geometry::FromText(char *text, int srid) {
	Postgis postgis;
	return postgis.LWGEOM_from_text(text, srid);
}

GSERIALIZED *Geometry::FromWKB(const char *text, size_t byte_size) {
	Postgis postgis;
	return postgis.LWGEOM_from_WKB(text, byte_size);
}

GSERIALIZED *Geometry::FromWKB(const char *text, size_t byte_size, int srid) {
	Postgis postgis;
	return postgis.LWGEOM_from_WKB(text, byte_size, srid);
}

double Geometry::XPoint(const void *data, size_t size) {
	Postgis postgis;
	return postgis.LWGEOM_x_point(data, size);
}

GSERIALIZED *Geometry::Centroid(GSERIALIZED *g) {
	Postgis postgis;
	return postgis.centroid(g);
}

GSERIALIZED *Geometry::Centroid(GSERIALIZED *g, bool use_spheroid) {
	Postgis postgis;
	return postgis.geography_centroid(g, use_spheroid);
}

} // namespace duckdb
