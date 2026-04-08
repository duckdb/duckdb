//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/load_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
#include <string>

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class Deserializer;
class Serializer;

enum class LoadType : uint8_t { LOAD, INSTALL, FORCE_INSTALL };

struct LoadInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::LOAD_INFO;

public:
	LoadInfo() : ParseInfo(TYPE) {
	}

	string filename;
	string repository;
	bool repo_is_alias;
	string version;
	LoadType load_type;

public:
	unique_ptr<LoadInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
