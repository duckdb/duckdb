#include "duckdb/parser/parsed_data/create_coordinate_system_info.hpp"
#include "duckdb/catalog/default/default_coordinate_systems.hpp"
#include "duckdb/catalog/catalog_entry/coordinate_system_catalog_entry.hpp"
#include "duckdb/common/array.hpp"

namespace duckdb {
class CoordinateSystemCatalogEntry;

namespace {

struct DefaultCoordinateReferenceSystem {
	const char *name;
	const char *auth_code;
	const char *srid;
	const char *wkt2_2019;
	const char *projjson;
};

using builtin_crs_array = std::array<DefaultCoordinateReferenceSystem, 2>;

// CRS84 (WGS84)
const auto OGC_CRS84_WKT2_2019 =
    R"WKT_LITERAL(GEOGCRS["WGS 84 (CRS84)",ENSEMBLE["World Geodetic System 1984 ensemble",MEMBER["World Geodetic System 1984 (Transit)"],MEMBER["World Geodetic System 1984 (G730)"],MEMBER["World Geodetic System 1984 (G873)"],MEMBER["World Geodetic System 1984 (G1150)"],MEMBER["World Geodetic System 1984 (G1674)"],MEMBER["World Geodetic System 1984 (G1762)"],MEMBER["World Geodetic System 1984 (G2139)"],MEMBER["World Geodetic System 1984 (G2296)"],ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]],ENSEMBLEACCURACY[2.0]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["geodetic longitude (Lon)",east,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["geodetic latitude (Lat)",north,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],USAGE[SCOPE["Not known."],AREA["World."],BBOX[-90,-180,90,180]],ID["OGC","CRS84"]])WKT_LITERAL";

const auto OGC_CRS84_PROJJSON =
    R"JSON_LITERAL({"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"WGS 84 (CRS84)","datum_ensemble":{"name":"World Geodetic System 1984 ensemble","members":[{"name":"World Geodetic System 1984 (Transit)","id":{"authority":"EPSG","code":1166}},{"name":"World Geodetic System 1984 (G730)","id":{"authority":"EPSG","code":1152}},{"name":"World Geodetic System 1984 (G873)","id":{"authority":"EPSG","code":1153}},{"name":"World Geodetic System 1984 (G1150)","id":{"authority":"EPSG","code":1154}},{"name":"World Geodetic System 1984 (G1674)","id":{"authority":"EPSG","code":1155}},{"name":"World Geodetic System 1984 (G1762)","id":{"authority":"EPSG","code":1156}},{"name":"World Geodetic System 1984 (G2139)","id":{"authority":"EPSG","code":1309}},{"name":"World Geodetic System 1984 (G2296)","id":{"authority":"EPSG","code":1383}}],"ellipsoid":{"name":"WGS 84","semi_major_axis":6378137,"inverse_flattening":298.257223563},"accuracy":"2.0","id":{"authority":"EPSG","code":6326}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"},{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"}]},"scope":"Not known.","area":"World.","bbox":{"south_latitude":-90,"west_longitude":-180,"north_latitude":90,"east_longitude":180},"id":{"authority":"OGC","code":"CRS84"}})JSON_LITERAL";

// CRS83 (NAD83)
const auto OGC_CRS83_WKT2_2019 =
    R"WKT_LITERAL(GEOGCRS["NAD83 (CRS83)",DATUM["North American Datum 1983",ELLIPSOID["GRS 1980",6378137,298.257222101,LENGTHUNIT["metre",1]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["geodetic longitude (Lon)",east,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["geodetic latitude (Lat)",north,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],USAGE[SCOPE["Not known."],AREA["North America - onshore and offshore: Canada - Alberta; British Columbia; Manitoba; New Brunswick; Newfoundland and Labrador; Northwest Territories; Nova Scotia; Nunavut; Ontario; Prince Edward Island; Quebec; Saskatchewan; Yukon. Puerto Rico. United States (USA) - Alabama; Alaska; Arizona; Arkansas; California; Colorado; Connecticut; Delaware; Florida; Georgia; Hawaii; Idaho; Illinois; Indiana; Iowa; Kansas; Kentucky; Louisiana; Maine; Maryland; Massachusetts; Michigan; Minnesota; Mississippi; Missouri; Montana; Nebraska; Nevada; New Hampshire; New Jersey; New Mexico; New York; North Carolina; North Dakota; Ohio; Oklahoma; Oregon; Pennsylvania; Rhode Island; South Carolina; South Dakota; Tennessee; Texas; Utah; Vermont; Virginia; Washington; West Virginia; Wisconsin; Wyoming. US Virgin Islands. British Virgin Islands."],BBOX[14.92,167.65,86.45,-40.73]],ID["OGC","CRS83"]])WKT_LITERAL";

const auto OGC_CRS83_PROJJSON =
    R"JSON_LITERAL({"$schema":"https://proj.org/schemas/v0.7/projjson.schema.json","type":"GeographicCRS","name":"NAD83 (CRS83)","datum":{"type":"GeodeticReferenceFrame","name":"North American Datum 1983","ellipsoid":{"name":"GRS 1980","semi_major_axis":6378137,"inverse_flattening":298.257222101}},"coordinate_system":{"subtype":"ellipsoidal","axis":[{"name":"Geodetic longitude","abbreviation":"Lon","direction":"east","unit":"degree"},{"name":"Geodetic latitude","abbreviation":"Lat","direction":"north","unit":"degree"}]},"scope":"Not known.","area":"North America - onshore and offshore: Canada - Alberta; British Columbia; Manitoba; New Brunswick; Newfoundland and Labrador; Northwest Territories; Nova Scotia; Nunavut; Ontario; Prince Edward Island; Quebec; Saskatchewan; Yukon. Puerto Rico. United States (USA) - Alabama; Alaska; Arizona; Arkansas; California; Colorado; Connecticut; Delaware; Florida; Georgia; Hawaii; Idaho; Illinois; Indiana; Iowa; Kansas; Kentucky; Louisiana; Maine; Maryland; Massachusetts; Michigan; Minnesota; Mississippi; Missouri; Montana; Nebraska; Nevada; New Hampshire; New Jersey; New Mexico; New York; North Carolina; North Dakota; Ohio; Oklahoma; Oregon; Pennsylvania; Rhode Island; South Carolina; South Dakota; Tennessee; Texas; Utah; Vermont; Virginia; Washington; West Virginia; Wisconsin; Wyoming. US Virgin Islands. British Virgin Islands.","bbox":{"south_latitude":14.92,"west_longitude":167.65,"north_latitude":86.45,"east_longitude":-40.73},"id":{"authority":"OGC","code":"CRS83"}})JSON_LITERAL";

const builtin_crs_array DEFAULT_CRS_DEFINITIONS = {{
    DefaultCoordinateReferenceSystem {"OGC:CRS84", "OGC", "CRS84", OGC_CRS84_WKT2_2019, OGC_CRS84_PROJJSON},
    DefaultCoordinateReferenceSystem {"OGC:CRS83", "OGC", "CRS83", OGC_CRS83_WKT2_2019, OGC_CRS83_PROJJSON},
}};

} // namespace

DefaultCoordinateSystemGenerator::DefaultCoordinateSystemGenerator(Catalog &catalog, SchemaCatalogEntry &schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultCoordinateSystemGenerator::CreateDefaultEntry(ClientContext &context,
                                                                              const string &entry_name) {
	if (schema.name != DEFAULT_SCHEMA) {
		return nullptr;
	}

	for (const auto &crs_definition : DEFAULT_CRS_DEFINITIONS) {
		if (entry_name == crs_definition.name) {
			CreateCoordinateSystemInfo info(crs_definition.name, crs_definition.auth_code, crs_definition.srid,
			                                crs_definition.projjson, crs_definition.wkt2_2019);

			auto result = make_uniq<CoordinateSystemCatalogEntry>(catalog, schema, info);
			return std::move(result);
		}
	}

	return nullptr;
}

vector<string> DefaultCoordinateSystemGenerator::GetDefaultEntries() {
	if (schema.name != DEFAULT_SCHEMA) {
		return {};
	}

	vector<string> entries;
	for (const auto &crs_definition : DEFAULT_CRS_DEFINITIONS) {
		entries.push_back(crs_definition.name);
	}
	return entries;
}

} // namespace duckdb
