#include "duckdb/parser/parsed_data/create_coordinate_system_info.hpp"

namespace duckdb {

CreateCoordinateSystemInfo::CreateCoordinateSystemInfo(string name_p, string authority, string code, string projjson,
                                                       string wkt2_2019)
    : CreateInfo(CatalogType::COORDINATE_SYSTEM_ENTRY), name(std::move(name_p)), authority(std::move(authority)),
      code(std::move(code)), projjson_definition(std::move(projjson)), wkt2_2019_definition(std::move(wkt2_2019)) {
	internal = true;
}

unique_ptr<CreateInfo> CreateCoordinateSystemInfo::Copy() const {
	auto result =
	    make_uniq<CreateCoordinateSystemInfo>(name, authority, code, projjson_definition, wkt2_2019_definition);
	CopyProperties(*result);
	return std::move(result);
}

} // namespace duckdb
