//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/vacuum_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

enum class LoadType { LOAD, INSTALL, FORCE_INSTALL };

struct LoadInfo : public ParseInfo {
	std::string filename;
	LoadType load_type;

public:
	unique_ptr<LoadInfo> Copy() const {
		auto result = make_uniq<LoadInfo>();
		result->filename = filename;
		result->load_type = load_type;
		return result;
	}

	void Serialize(Serializer &serializer) const {
		FieldWriter writer(serializer);
		writer.WriteString(filename);
		writer.WriteField<LoadType>(load_type);
		writer.Finalize();
	}

	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer) {
		FieldReader reader(deserializer);
		auto load_info = make_uniq<LoadInfo>();
		load_info->filename = reader.ReadRequired<string>();
		load_info->load_type = reader.ReadRequired<LoadType>();
		reader.Finalize();
		return std::move(load_info);
	}
};

} // namespace duckdb
