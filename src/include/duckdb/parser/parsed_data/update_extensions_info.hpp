//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/update_extensions_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

struct UpdateExtensionsInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::UPDATE_EXTENSIONS_INFO;

public:
	UpdateExtensionsInfo() : ParseInfo(TYPE) {
	}

	vector<string> extensions_to_update;

public:
	unique_ptr<UpdateExtensionsInfo> Copy() const {
		auto result = make_uniq<UpdateExtensionsInfo>();
		result->extensions_to_update = extensions_to_update;
		return result;
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
